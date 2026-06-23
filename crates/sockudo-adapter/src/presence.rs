use crate::connection_manager::ConnectionManager;
use compact_str::{CompactString, format_compact};
use dashmap::DashMap;
use sockudo_core::app::App;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::presence_history::{
    PresenceHistoryEventCause, PresenceHistoryEventKind, PresenceHistoryRetentionPolicy,
    PresenceHistoryStore, PresenceHistoryTransitionRecord,
};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
use sockudo_webhook::WebhookIntegration;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, warn};

const MAX_PENDING_PRESENCE_REMOVALS: usize = 100_000;

/// Lock key for presence operations to prevent TOCTOU races
/// Format: "app_id:channel:user_id"
fn presence_lock_key(app_id: &str, channel: &str, user_id: &str) -> CompactString {
    format_compact!("{app_id}:{channel}:{user_id}")
}

type PresenceLockMap = DashMap<CompactString, Arc<Mutex<()>>, ahash::RandomState>;

struct PendingRemovalTask {
    connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
    presence_history_enabled: bool,
    webhook_integration: Option<Arc<WebhookIntegration>>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    app_config: App,
    channel: String,
    user_id: String,
    socket_for_leave: Option<String>,
    generation: u64,
    retention: Option<PresenceHistoryRetentionPolicy>,
}

/// Centralized presence channel management functionality
/// This module handles presence member removal logic that needs to be
/// consistent across different disconnect paths (sync, async, direct unsubscribe)
pub struct PresenceManager {
    /// Per-user locks to prevent TOCTOU races during presence operations
    /// Maps "app_id:channel:user_id" -> async mutex for true per-key exclusivity
    presence_locks: PresenceLockMap,
    removal_generation: AtomicU64,
    pending_removal_deadlines: Arc<Mutex<BTreeMap<Instant, VecDeque<PendingRemovalTask>>>>,
    pending_removal_count: Arc<AtomicUsize>,
    pending_removal_worker_started: AtomicBool,
    pending_removal_notify: Arc<Notify>,
}

impl Default for PresenceManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PresenceManager {
    pub fn new() -> Self {
        Self {
            presence_locks: DashMap::with_hasher(ahash::RandomState::new()),
            removal_generation: AtomicU64::new(1),
            pending_removal_deadlines: Arc::new(Mutex::new(BTreeMap::new())),
            pending_removal_count: Arc::new(AtomicUsize::new(0)),
            pending_removal_worker_started: AtomicBool::new(false),
            pending_removal_notify: Arc::new(Notify::new()),
        }
    }

    fn get_presence_lock(&self, lock_key: &CompactString) -> Arc<Mutex<()>> {
        self.presence_locks
            .entry(lock_key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn ensure_pending_removal_worker(&self) {
        if self
            .pending_removal_worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let deadlines = Arc::clone(&self.pending_removal_deadlines);
            let count = Arc::clone(&self.pending_removal_count);
            let notify = Arc::clone(&self.pending_removal_notify);
            tokio::spawn(async move {
                Self::run_pending_removal_worker(deadlines, count, notify).await;
            });
        }
    }

    async fn schedule_pending_removal(
        &self,
        delay_seconds: u64,
        task: PendingRemovalTask,
    ) -> Result<()> {
        if self.pending_removal_count.fetch_add(1, Ordering::AcqRel)
            >= MAX_PENDING_PRESENCE_REMOVALS
        {
            self.pending_removal_count.fetch_sub(1, Ordering::AcqRel);
            return Err(sockudo_core::error::Error::OverCapacity);
        }

        self.ensure_pending_removal_worker();
        let deadline = Instant::now() + Duration::from_secs(delay_seconds);
        {
            let mut deadlines = self.pending_removal_deadlines.lock().await;
            deadlines.entry(deadline).or_default().push_back(task);
        }
        self.pending_removal_notify.notify_one();
        Ok(())
    }

    async fn run_pending_removal_worker(
        deadlines: Arc<Mutex<BTreeMap<Instant, VecDeque<PendingRemovalTask>>>>,
        count: Arc<AtomicUsize>,
        notify: Arc<Notify>,
    ) {
        loop {
            let next_deadline = {
                let deadlines = deadlines.lock().await;
                deadlines.keys().next().copied()
            };

            let Some(deadline) = next_deadline else {
                notify.notified().await;
                continue;
            };

            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {}
                _ = notify.notified() => continue,
            }

            let due_tasks = Self::take_due_pending_removals(&deadlines, Instant::now()).await;
            for task in due_tasks {
                count.fetch_sub(1, Ordering::AcqRel);
                Self::process_pending_removal(task).await;
            }
        }
    }

    async fn take_due_pending_removals(
        deadlines: &Mutex<BTreeMap<Instant, VecDeque<PendingRemovalTask>>>,
        now: Instant,
    ) -> Vec<PendingRemovalTask> {
        let mut deadlines = deadlines.lock().await;
        let due_deadlines = deadlines
            .keys()
            .copied()
            .take_while(|deadline| *deadline <= now)
            .collect::<Vec<_>>();
        let mut tasks = Vec::new();
        for deadline in due_deadlines {
            if let Some(mut bucket) = deadlines.remove(&deadline) {
                tasks.extend(bucket.drain(..));
            }
        }
        tasks
    }

    async fn process_pending_removal(task: PendingRemovalTask) {
        let still_connected = Self::user_has_other_connections_in_presence_channel(
            Arc::clone(&task.connection_manager),
            &task.app_config.id,
            &task.channel,
            &task.user_id,
            None,
        )
        .await
        .unwrap_or(false);
        if still_connected {
            let _ = task
                .connection_manager
                .cancel_pending_presence_member(&task.app_config.id, &task.channel, &task.user_id)
                .await;
            return;
        }

        let pending = task
            .connection_manager
            .remove_pending_presence_member(
                &task.app_config.id,
                &task.channel,
                &task.user_id,
                task.generation,
            )
            .await
            .ok()
            .flatten();
        if pending.is_none() {
            return;
        }

        if let Some(socket_id) = task.socket_for_leave.as_deref()
            && let Some(horizontal_adapter) = task.connection_manager.as_horizontal_adapter()
        {
            horizontal_adapter
                .broadcast_presence_leave(
                    &task.app_config.id,
                    &task.channel,
                    &task.user_id,
                    socket_id,
                )
                .await
                .ok();
        }

        let _ = Self::emit_member_removed_inner(
            &task.connection_manager,
            task.presence_history_store,
            task.presence_history_enabled,
            task.webhook_integration.as_ref(),
            task.metrics.as_ref(),
            &task.app_config,
            &task.channel,
            &task.user_id,
            None,
            PresenceHistoryEventCause::Timeout,
            None,
            task.retention,
        )
        .await;
    }

    /// Handles presence member addition including both webhook and broadcast
    /// Only sends events if this is the user's FIRST connection to the presence channel
    ///
    /// FIX: Uses atomic check-and-act pattern to prevent TOCTOU race conditions
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_member_added(
        &self,
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_enabled: bool,
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        app_config: &App,
        channel: &str,
        user_id: &str,
        user_info: Option<&sonic_rs::Value>,
        excluding_socket: Option<&SocketId>,
        retention: Option<sockudo_core::presence_history::PresenceHistoryRetentionPolicy>,
    ) -> Result<()> {
        debug!(
            "Processing presence member addition for user {} in channel {} (app: {})",
            user_id, channel, app_config.id
        );

        let lock_key = presence_lock_key(&app_config.id, channel, user_id);
        let presence_lock = self.get_presence_lock(&lock_key);

        // TOCTOU check AND member_added broadcasts under the per-user lock.
        // The lock serializes the count check so two concurrent first-joins cannot
        // both see count=0 and emit duplicate member_added events.
        // The broadcast must stay inside the lock: a concurrent last-leave/first-join
        // race on the same user emits both member_removed and member_added, and the
        // wire order must match the decision order or clients end up inverted.
        // Webhook enqueue and history recording happen after the lock is released.
        let (should_send_event, had_other_connections, had_pending_removal) = {
            let _lock_guard = presence_lock.lock().await;

            let cancelled_pending_socket = connection_manager
                .cancel_pending_presence_member(&app_config.id, channel, user_id)
                .await?;
            if let Some(old_socket_id) = cancelled_pending_socket.as_deref()
                && let Some(horizontal_adapter) = connection_manager.as_horizontal_adapter()
            {
                horizontal_adapter
                    .broadcast_presence_leave(&app_config.id, channel, user_id, old_socket_id)
                    .await
                    .ok();
            }

            let had_other_connections = Self::user_has_other_connections_in_presence_channel(
                Arc::clone(&connection_manager),
                &app_config.id,
                channel,
                user_id,
                excluding_socket,
            )
            .await?;

            let should_send_event = !had_other_connections && cancelled_pending_socket.is_none();

            if should_send_event {
                debug!(
                    "User {} is joining channel {} for the first time, sending member_added events",
                    user_id, channel
                );

                let member_added_msg = sockudo_protocol::messages::PusherMessage::member_added(
                    channel.to_string(),
                    user_id.to_string(),
                    user_info.cloned(),
                );
                Self::broadcast_to_channel(
                    Arc::clone(&connection_manager),
                    &app_config.id,
                    channel,
                    member_added_msg,
                    excluding_socket,
                )
                .await?;

                let _ = Self::broadcast_to_channel(
                    Arc::clone(&connection_manager),
                    &app_config.id,
                    &format!("[meta]{channel}"),
                    sockudo_protocol::messages::PusherMessage {
                        event: Some("sockudo_internal:member_added".to_string()),
                        channel: Some(format!("[meta]{channel}")),
                        data: Some(sockudo_protocol::messages::MessageData::Json(
                            sonic_rs::json!({
                                "channel": channel,
                                "user_id": user_id,
                            }),
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
                    },
                    None,
                )
                .await;
            }

            (
                should_send_event,
                had_other_connections,
                cancelled_pending_socket.is_some(),
            )
        };

        if should_send_event {
            if let Some(webhook_integration) = webhook_integration
                && let Err(e) = webhook_integration
                    .send_member_added(app_config, channel, user_id)
                    .await
            {
                warn!(
                    "Failed to send member_added webhook for user {} in channel {}: {}",
                    user_id, channel, e
                );
            }

            debug!(
                "Successfully processed member_added for user {} in channel {}",
                user_id, channel
            );

            if presence_history_enabled {
                self.record_presence_transition(
                    presence_history_store,
                    metrics,
                    PresenceHistoryTransitionRecord {
                        app_id: app_config.id.clone(),
                        channel: channel.to_string(),
                        event_kind: PresenceHistoryEventKind::MemberAdded,
                        cause: PresenceHistoryEventCause::Join,
                        user_id: user_id.to_string(),
                        connection_id: excluding_socket.map(ToString::to_string),
                        user_info: user_info.cloned(),
                        dead_node_id: None,
                        dedupe_key: format!(
                            "member_added:{}:{}:{}:{}",
                            app_config.id,
                            channel,
                            user_id,
                            excluding_socket
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "_".to_string())
                        ),
                        published_at_ms: sockudo_core::history::now_ms(),
                        retention: retention.clone().unwrap_or(
                            sockudo_core::presence_history::PresenceHistoryRetentionPolicy {
                                retention_window_seconds: 3600,
                                max_events_per_channel: None,
                                max_bytes_per_channel: None,
                            },
                        ),
                    },
                )
                .await;
            }
        } else {
            debug!(
                "User {} already has {} connections or pending removal in channel {}, skipping member_added events",
                user_id,
                if had_other_connections || had_pending_removal {
                    "other"
                } else {
                    "no"
                },
                channel
            );
        }

        // Always broadcast presence join to all nodes for cluster replication (for every connection)
        // This is done OUTSIDE the lock to avoid holding it during network I/O
        if let Some(excluding_socket) = excluding_socket
            && let Some(horizontal_adapter) = connection_manager.as_horizontal_adapter()
        {
            horizontal_adapter
                .broadcast_presence_join(
                    &app_config.id,
                    channel,
                    user_id,
                    &excluding_socket.to_string(),
                    user_info.cloned(),
                )
                .await
                .map_err(|e| {
                    error!("Failed to broadcast presence join: {}", e);
                    e
                })
                .ok(); // Log but don't fail the operation
        }

        // Opportunistically cap lock-map growth on high-cardinality presence churn.
        self.cleanup_stale_locks();

        Ok(())
    }

    /// Handles presence member removal including both webhook and broadcast
    /// This centralizes the logic that was duplicated across sync cleanup,
    /// async cleanup, and direct unsubscribe paths
    ///
    /// FIX: Uses atomic check-and-act pattern to prevent TOCTOU race conditions
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_member_removed(
        &self,
        connection_manager: &Arc<dyn ConnectionManager + Send + Sync>,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_enabled: bool,
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        app_config: &App,
        channel: &str,
        user_id: &str,
        excluding_socket: Option<&SocketId>,
        cause: PresenceHistoryEventCause,
        dead_node_id: Option<&str>,
        ungraceful_timeout_seconds: u64,
        retention: Option<sockudo_core::presence_history::PresenceHistoryRetentionPolicy>,
    ) -> Result<()> {
        debug!(
            "Processing presence member removal for user {} in channel {} (app: {})",
            user_id, channel, app_config.id
        );

        let lock_key = presence_lock_key(&app_config.id, channel, user_id);

        let presence_lock = self.get_presence_lock(&lock_key);

        // TOCTOU check AND member_removed broadcasts under the per-user lock.
        // The lock serializes the count check so a disconnect racing a same-user
        // connect cannot emit member_removed for a user that is still present.
        // The broadcast must stay inside the lock so same-user leave/join wire
        // order matches the decision order.
        // Webhook enqueue and history recording happen after the lock is released.
        let should_send_event = {
            let _lock_guard = presence_lock.lock().await;

            let has_other_connections = Self::user_has_other_connections_in_presence_channel(
                Arc::clone(connection_manager),
                &app_config.id,
                channel,
                user_id,
                excluding_socket,
            )
            .await?;

            if !has_other_connections {
                if cause == PresenceHistoryEventCause::Disconnect && ungraceful_timeout_seconds > 0
                {
                    let generation = self.removal_generation.fetch_add(1, Ordering::Relaxed);
                    let user_info = if let Some(socket_id) = excluding_socket {
                        connection_manager
                            .get_presence_member(&app_config.id, channel, socket_id)
                            .await
                            .and_then(|member| member.user_info)
                    } else {
                        None
                    };
                    connection_manager
                        .mark_presence_member_pending(
                            &app_config.id,
                            channel,
                            user_id,
                            &excluding_socket
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "_".to_string()),
                            user_info,
                            generation,
                        )
                        .await?;

                    self.schedule_pending_removal(
                        ungraceful_timeout_seconds,
                        PendingRemovalTask {
                            connection_manager: Arc::clone(connection_manager),
                            presence_history_store: Arc::clone(&presence_history_store),
                            presence_history_enabled,
                            webhook_integration: webhook_integration.cloned(),
                            metrics: metrics.cloned(),
                            app_config: app_config.clone(),
                            channel: channel.to_string(),
                            user_id: user_id.to_string(),
                            socket_for_leave: excluding_socket.map(ToString::to_string),
                            generation,
                            retention: retention.clone(),
                        },
                    )
                    .await?;

                    self.cleanup_stale_locks();
                    return Ok(());
                }

                debug!(
                    "User {} has no other connections in channel {}, sending member_removed events",
                    user_id, channel
                );

                let member_removed_msg =
                    PusherMessage::member_removed(channel.to_string(), user_id.to_string());
                Self::broadcast_to_channel(
                    Arc::clone(connection_manager),
                    &app_config.id,
                    channel,
                    member_removed_msg,
                    excluding_socket,
                )
                .await?;

                let _ = Self::broadcast_to_channel(
                    Arc::clone(connection_manager),
                    &app_config.id,
                    &format!("[meta]{channel}"),
                    PusherMessage {
                        event: Some("sockudo_internal:member_removed".to_string()),
                        channel: Some(format!("[meta]{channel}")),
                        data: Some(sockudo_protocol::messages::MessageData::Json(
                            sonic_rs::json!({
                                "channel": channel,
                                "user_id": user_id,
                            }),
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
                    },
                    None,
                )
                .await;

                true
            } else {
                debug!(
                    "User {} has other connections in channel {}, skipping member_removed events",
                    user_id, channel
                );
                false
            }
        };

        if should_send_event {
            if let Some(webhook_integration) = webhook_integration {
                let wi = Arc::clone(webhook_integration);
                let cm = Arc::clone(connection_manager);
                let app = app_config.clone();
                let ch = channel.to_string();
                let uid = user_id.to_string();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    let still_gone = match Self::user_has_other_connections_in_presence_channel(
                        cm, &app.id, &ch, &uid, None,
                    )
                    .await
                    {
                        Ok(has_connections) => !has_connections,
                        Err(_) => true,
                    };
                    if still_gone && let Err(e) = wi.send_member_removed(&app, &ch, &uid).await {
                        tracing::warn!(
                            "Failed to send member_removed webhook for user {} in channel {}: {}",
                            uid,
                            ch,
                            e
                        );
                    }
                });
            }

            debug!(
                "Successfully processed member_removed for user {} in channel {}",
                user_id, channel
            );

            if presence_history_enabled {
                self.record_presence_transition(
                    presence_history_store,
                    metrics,
                    PresenceHistoryTransitionRecord {
                        app_id: app_config.id.clone(),
                        channel: channel.to_string(),
                        event_kind: PresenceHistoryEventKind::MemberRemoved,
                        cause,
                        user_id: user_id.to_string(),
                        connection_id: excluding_socket.map(ToString::to_string),
                        user_info: None,
                        dead_node_id: dead_node_id.map(ToString::to_string),
                        dedupe_key: format!(
                            "member_removed:{}:{}:{}:{}:{}",
                            app_config.id,
                            channel,
                            user_id,
                            excluding_socket
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "_".to_string()),
                            dead_node_id.unwrap_or("_")
                        ),
                        published_at_ms: sockudo_core::history::now_ms(),
                        retention: retention.clone().unwrap_or(
                            sockudo_core::presence_history::PresenceHistoryRetentionPolicy {
                                retention_window_seconds: 3600,
                                max_events_per_channel: None,
                                max_bytes_per_channel: None,
                            },
                        ),
                    },
                )
                .await;
            }
        }

        // Always broadcast presence leave to all nodes for cluster replication (for every disconnection)
        // This is done OUTSIDE the lock to avoid holding it during network I/O
        if let Some(excluding_socket) = excluding_socket
            && let Some(horizontal_adapter) = connection_manager.as_horizontal_adapter()
        {
            horizontal_adapter
                .broadcast_presence_leave(
                    &app_config.id,
                    channel,
                    user_id,
                    &excluding_socket.to_string(),
                )
                .await
                .map_err(|e| {
                    error!("Failed to broadcast presence leave: {}", e);
                    e
                })
                .ok(); // Log but don't fail the operation
        }

        // Opportunistically cap lock-map growth on high-cardinality presence churn.
        self.cleanup_stale_locks();

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn handle_member_updated(
        &self,
        connection_manager: &Arc<dyn ConnectionManager + Send + Sync>,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_enabled: bool,
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        app_config: &App,
        channel: &str,
        socket_id: &SocketId,
        user_id: &str,
        user_info: sonic_rs::Value,
        retention: Option<sockudo_core::presence_history::PresenceHistoryRetentionPolicy>,
    ) -> Result<()> {
        let lock_key = presence_lock_key(&app_config.id, channel, user_id);
        let presence_lock = self.get_presence_lock(&lock_key);
        let _lock_guard = presence_lock.lock().await;

        let Some(updated_member) = connection_manager
            .update_presence_member(&app_config.id, channel, socket_id, user_info.clone())
            .await?
        else {
            return Err(sockudo_core::error::Error::InvalidMessageFormat(
                "presence_update requires an active channel membership".to_string(),
            ));
        };

        connection_manager
            .cancel_pending_presence_member(&app_config.id, channel, user_id)
            .await?;

        if let Some(horizontal_adapter) = connection_manager.as_horizontal_adapter() {
            horizontal_adapter
                .broadcast_presence_update(
                    &app_config.id,
                    channel,
                    user_id,
                    &socket_id.to_string(),
                    user_info.clone(),
                )
                .await
                .ok();
        }

        if let Some(webhook_integration) = webhook_integration
            && let Err(e) = webhook_integration
                .send_member_updated(app_config, channel, user_id, user_info.clone())
                .await
        {
            warn!(
                "Failed to send member_updated webhook for user {} in channel {}: {}",
                user_id, channel, e
            );
        }

        let update_msg = PusherMessage::member_updated(
            channel.to_string(),
            user_id.to_string(),
            user_info.clone(),
        );
        Self::broadcast_to_channel(
            Arc::clone(connection_manager),
            &app_config.id,
            channel,
            update_msg,
            None,
        )
        .await?;

        let _ = Self::broadcast_to_channel(
            Arc::clone(connection_manager),
            &app_config.id,
            &format!("[meta]{channel}"),
            PusherMessage {
                event: Some("sockudo_internal:presence_update".to_string()),
                channel: Some(format!("[meta]{channel}")),
                data: Some(sockudo_protocol::messages::MessageData::Json(
                    sonic_rs::json!({
                        "channel": channel,
                        "user_id": user_id,
                    }),
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
            },
            None,
        )
        .await;

        if presence_history_enabled {
            self.record_presence_transition(
                presence_history_store,
                metrics,
                PresenceHistoryTransitionRecord {
                    app_id: app_config.id.clone(),
                    channel: channel.to_string(),
                    event_kind: PresenceHistoryEventKind::MemberUpdated,
                    cause: PresenceHistoryEventCause::Join,
                    user_id: user_id.to_string(),
                    connection_id: Some(socket_id.to_string()),
                    user_info: updated_member.user_info,
                    dead_node_id: None,
                    dedupe_key: format!(
                        "member_updated:{}:{}:{}:{}",
                        app_config.id, channel, user_id, socket_id
                    ),
                    published_at_ms: sockudo_core::history::now_ms(),
                    retention: retention.unwrap_or(
                        sockudo_core::presence_history::PresenceHistoryRetentionPolicy {
                            retention_window_seconds: 3600,
                            max_events_per_channel: None,
                            max_bytes_per_channel: None,
                        },
                    ),
                },
            )
            .await;
        }

        self.cleanup_stale_locks();
        Ok(())
    }

    /// Cleanup stale lock entries periodically
    /// Call this from a background task to prevent unbounded growth of the locks map
    pub fn cleanup_stale_locks(&self) {
        if self.presence_locks.len() > 100_000 {
            let stale_keys: Vec<CompactString> = self
                .presence_locks
                .iter()
                .filter_map(|entry| {
                    let lock = entry.value();
                    let in_use = Arc::strong_count(lock) > 1 || lock.try_lock().is_err();
                    if in_use {
                        None
                    } else {
                        Some(entry.key().clone())
                    }
                })
                .collect();

            warn!(
                "Presence locks map has {} entries, removing {} stale entries",
                self.presence_locks.len(),
                stale_keys.len()
            );

            for key in stale_keys {
                self.presence_locks.remove(&key);
            }
        }
    }

    /// Check if a user has other connections in a presence channel
    /// Uses the same logic as the original working implementation:
    /// Get all user's sockets and check if any are still subscribed to this channel (excluding specified socket)
    async fn user_has_other_connections_in_presence_channel(
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        app_id: &str,
        channel: &str,
        user_id: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<bool> {
        connection_manager
            .user_has_connections_in_channel(user_id, app_id, channel, excluding_socket)
            .await
    }

    /// Broadcast a message to all clients in a channel, optionally excluding one socket
    async fn broadcast_to_channel(
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        app_id: &str,
        channel: &str,
        message: PusherMessage,
        excluding_socket: Option<&SocketId>,
    ) -> Result<()> {
        connection_manager
            .send(channel, message, excluding_socket, app_id, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_member_removed_inner(
        connection_manager: &Arc<dyn ConnectionManager + Send + Sync>,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_enabled: bool,
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        app_config: &App,
        channel: &str,
        user_id: &str,
        excluding_socket: Option<&SocketId>,
        cause: PresenceHistoryEventCause,
        dead_node_id: Option<&str>,
        retention: Option<sockudo_core::presence_history::PresenceHistoryRetentionPolicy>,
    ) -> Result<()> {
        if let Some(webhook_integration) = webhook_integration {
            let wi = Arc::clone(webhook_integration);
            let cm = Arc::clone(connection_manager);
            let app = app_config.clone();
            let ch = channel.to_string();
            let uid = user_id.to_string();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                let still_gone = match Self::user_has_other_connections_in_presence_channel(
                    cm, &app.id, &ch, &uid, None,
                )
                .await
                {
                    Ok(has_connections) => !has_connections,
                    Err(_) => true,
                };
                if still_gone && let Err(e) = wi.send_member_removed(&app, &ch, &uid).await {
                    tracing::warn!(
                        "Failed to send member_removed webhook for user {} in channel {}: {}",
                        uid,
                        ch,
                        e
                    );
                }
            });
        }

        let member_removed_msg =
            PusherMessage::member_removed(channel.to_string(), user_id.to_string());
        Self::broadcast_to_channel(
            Arc::clone(connection_manager),
            &app_config.id,
            channel,
            member_removed_msg,
            excluding_socket,
        )
        .await?;

        let _ = Self::broadcast_to_channel(
            Arc::clone(connection_manager),
            &app_config.id,
            &format!("[meta]{channel}"),
            PusherMessage {
                event: Some("sockudo_internal:member_removed".to_string()),
                channel: Some(format!("[meta]{channel}")),
                data: Some(sockudo_protocol::messages::MessageData::Json(
                    sonic_rs::json!({
                        "channel": channel,
                        "user_id": user_id,
                    }),
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
            },
            None,
        )
        .await;

        debug!(
            "Successfully processed member_removed for user {} in channel {}",
            user_id, channel
        );

        if presence_history_enabled {
            Self::record_presence_transition_inner(
                presence_history_store,
                metrics,
                PresenceHistoryTransitionRecord {
                    app_id: app_config.id.clone(),
                    channel: channel.to_string(),
                    event_kind: PresenceHistoryEventKind::MemberRemoved,
                    cause,
                    user_id: user_id.to_string(),
                    connection_id: excluding_socket.map(ToString::to_string),
                    user_info: None,
                    dead_node_id: dead_node_id.map(ToString::to_string),
                    dedupe_key: format!(
                        "member_removed:{}:{}:{}:{}:{}",
                        app_config.id,
                        channel,
                        user_id,
                        excluding_socket
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "_".to_string()),
                        dead_node_id.unwrap_or("_")
                    ),
                    published_at_ms: sockudo_core::history::now_ms(),
                    retention: retention.unwrap_or(
                        sockudo_core::presence_history::PresenceHistoryRetentionPolicy {
                            retention_window_seconds: 3600,
                            max_events_per_channel: None,
                            max_bytes_per_channel: None,
                        },
                    ),
                },
            )
            .await;
        }

        Ok(())
    }

    async fn record_presence_transition(
        &self,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        record: PresenceHistoryTransitionRecord,
    ) {
        Self::record_presence_transition_inner(presence_history_store, metrics, record).await;
    }

    async fn record_presence_transition_inner(
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        record: PresenceHistoryTransitionRecord,
    ) {
        if let Err(error) = presence_history_store
            .record_transition(record.clone())
            .await
        {
            warn!(
                "Failed to record presence history transition for user {} in channel {}: {}",
                record.user_id, record.channel, error
            );
            if let Some(metrics) = metrics {
                metrics.mark_presence_history_write_failure(&record.app_id);
            }
        }
    }
}

/// Static helper functions for backward compatibility with code that doesn't have
/// access to a PresenceManager instance. These use a global lock map.
///
/// NOTE: Prefer using the instance methods when possible for better testability
/// and to allow cleanup of stale locks.
mod static_helpers {
    use super::*;
    use std::sync::LazyLock;

    /// Global presence manager for static helper functions
    static GLOBAL_PRESENCE_MANAGER: LazyLock<PresenceManager> = LazyLock::new(PresenceManager::new);

    /// Get the global presence manager instance
    pub fn global() -> &'static PresenceManager {
        &GLOBAL_PRESENCE_MANAGER
    }
}

/// Re-export static helper access for backward compatibility
pub fn global_presence_manager() -> &'static PresenceManager {
    static_helpers::global()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashMap;
    use async_trait::async_trait;
    use sockudo_core::app::{App, AppPolicy};
    use sockudo_core::channel::PresenceMemberInfo;
    use sockudo_core::namespace::Namespace;
    use sockudo_core::presence_history::{
        MemoryPresenceHistoryStore, PresenceHistoryDirection, PresenceHistoryEventCause,
        PresenceHistoryEventKind, PresenceHistoryReadRequest, PresenceHistoryRetentionPolicy,
    };
    use sockudo_protocol::WireFormat;
    use sockudo_ws::axum_integration::WebSocketWriter;
    use std::any::Any;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    #[test]
    fn test_presence_lock_key_format() {
        let key = presence_lock_key("app123", "presence-room", "user456");
        assert_eq!(key, "app123:presence-room:user456");
    }

    #[test]
    fn test_presence_manager_creation() {
        let manager = PresenceManager::new();
        assert_eq!(manager.presence_locks.len(), 0);
    }

    #[test]
    fn test_cleanup_stale_locks_threshold() {
        let manager = PresenceManager::new();

        // Add entries below threshold
        for i in 0..100 {
            manager
                .presence_locks
                .insert(format_compact!("key_{i}"), Arc::new(Mutex::new(())));
        }

        manager.cleanup_stale_locks();
        // Should not clear because below 100_000 threshold
        assert_eq!(manager.presence_locks.len(), 100);
    }

    struct ScriptedConnectionManager {
        counts: StdMutex<VecDeque<usize>>,
    }

    impl ScriptedConnectionManager {
        fn new(counts: Vec<usize>) -> Self {
            Self {
                counts: StdMutex::new(counts.into()),
            }
        }
    }

    #[async_trait]
    impl ConnectionManager for ScriptedConnectionManager {
        async fn init(&self) {}
        async fn get_namespace(&self, _app_id: &str) -> Option<Arc<Namespace>> {
            None
        }
        async fn add_socket(
            &self,
            _socket_id: SocketId,
            _socket: WebSocketWriter,
            _app_id: &str,
            _app_manager: Arc<dyn sockudo_core::app::AppManager + Send + Sync>,
            _buffer_config: sockudo_core::websocket::WebSocketBufferConfig,
            _protocol_version: sockudo_protocol::ProtocolVersion,
            _wire_format: WireFormat,
            _echo_messages: bool,
        ) -> Result<()> {
            Ok(())
        }
        async fn get_connection(
            &self,
            _socket_id: &SocketId,
            _app_id: &str,
        ) -> Option<sockudo_core::websocket::WebSocketRef> {
            None
        }
        async fn remove_connection(&self, _socket_id: &SocketId, _app_id: &str) -> Result<()> {
            Ok(())
        }
        async fn send_message(
            &self,
            _app_id: &str,
            _socket_id: &SocketId,
            _message: PusherMessage,
        ) -> Result<()> {
            Ok(())
        }
        async fn send(
            &self,
            _channel: &str,
            _message: PusherMessage,
            _except: Option<&SocketId>,
            _app_id: &str,
            _start_time_ms: Option<f64>,
        ) -> Result<()> {
            Ok(())
        }
        async fn get_channel_members(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> Result<AHashMap<String, PresenceMemberInfo>> {
            Ok(AHashMap::new())
        }
        async fn get_channel_sockets(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> Result<Vec<SocketId>> {
            Ok(Vec::new())
        }
        async fn remove_channel(&self, _app_id: &str, _channel: &str) {}
        async fn is_in_channel(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) -> Result<bool> {
            Ok(false)
        }
        async fn get_user_sockets(
            &self,
            _user_id: &str,
            _app_id: &str,
        ) -> Result<Vec<sockudo_core::websocket::WebSocketRef>> {
            Ok(Vec::new())
        }
        async fn cleanup_connection(
            &self,
            _app_id: &str,
            _ws: sockudo_core::websocket::WebSocketRef,
        ) {
        }
        async fn terminate_connection(&self, _app_id: &str, _user_id: &str) -> Result<()> {
            Ok(())
        }
        async fn add_channel_to_sockets(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) {
        }
        async fn get_channel_socket_count_info(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> crate::connection_manager::ChannelSocketCount {
            crate::connection_manager::ChannelSocketCount {
                count: 0,
                complete: true,
            }
        }
        async fn get_channel_socket_count(&self, _app_id: &str, _channel: &str) -> usize {
            0
        }
        async fn add_to_channel(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) -> Result<(bool, bool)> {
            Ok((false, false))
        }
        async fn remove_from_channel(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) -> Result<(bool, bool)> {
            Ok((false, false))
        }
        async fn get_presence_member(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) -> Option<PresenceMemberInfo> {
            None
        }
        async fn terminate_user_connections(&self, _app_id: &str, _user_id: &str) -> Result<()> {
            Ok(())
        }
        async fn add_user(&self, _ws: sockudo_core::websocket::WebSocketRef) -> Result<()> {
            Ok(())
        }
        async fn remove_user(&self, _ws: sockudo_core::websocket::WebSocketRef) -> Result<()> {
            Ok(())
        }
        async fn remove_user_socket(
            &self,
            _user_id: &str,
            _socket_id: &SocketId,
            _app_id: &str,
        ) -> Result<()> {
            Ok(())
        }
        async fn count_user_connections_in_channel(
            &self,
            _user_id: &str,
            _app_id: &str,
            _channel: &str,
            _excluding_socket: Option<&SocketId>,
        ) -> Result<usize> {
            Ok(self.counts.lock().unwrap().pop_front().unwrap_or_default())
        }
        async fn get_channels_with_socket_count(
            &self,
            _app_id: &str,
        ) -> Result<AHashMap<String, usize>> {
            Ok(AHashMap::new())
        }
        async fn get_sockets_count(&self, _app_id: &str) -> Result<usize> {
            Ok(0)
        }
        async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
            Ok(Vec::new())
        }
        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
        async fn check_health(&self) -> Result<()> {
            Ok(())
        }
        fn get_node_id(&self) -> String {
            "test-node".to_string()
        }
        fn as_horizontal_adapter(
            &self,
        ) -> Option<&dyn crate::connection_manager::HorizontalAdapterInterface> {
            None
        }
    }

    fn test_app() -> App {
        App::from_policy(
            "app-1".to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            AppPolicy::default(),
        )
    }

    fn default_retention() -> PresenceHistoryRetentionPolicy {
        PresenceHistoryRetentionPolicy {
            retention_window_seconds: 3600,
            max_events_per_channel: None,
            max_bytes_per_channel: None,
        }
    }

    #[tokio::test]
    async fn presence_manager_records_authoritative_join_and_leave_history() {
        let manager = PresenceManager::new();
        let connection_manager: Arc<dyn ConnectionManager + Send + Sync> =
            Arc::new(ScriptedConnectionManager::new(vec![0, 0]));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let app = test_app();
        let socket = SocketId::new();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                Some(&socket),
                PresenceHistoryEventCause::Disconnect,
                None,
                0,
                Some(default_retention()),
            )
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: app.id.clone(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
        assert_eq!(page.items[1].cause, PresenceHistoryEventCause::Disconnect);
    }

    #[tokio::test]
    async fn presence_manager_skips_non_authoritative_socket_churn_history() {
        let manager = PresenceManager::new();
        let connection_manager: Arc<dyn ConnectionManager + Send + Sync> =
            Arc::new(ScriptedConnectionManager::new(vec![0, 1, 1, 0]));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let app = test_app();
        let socket_a = SocketId::new();
        let socket_b = SocketId::new();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket_a),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket_b),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                Some(&socket_a),
                PresenceHistoryEventCause::Disconnect,
                None,
                0,
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                Some(&socket_b),
                PresenceHistoryEventCause::Disconnect,
                None,
                0,
                Some(default_retention()),
            )
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: app.id.clone(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
    }

    #[tokio::test]
    async fn presence_manager_suppresses_duplicate_authoritative_join_reports() {
        let manager = PresenceManager::new();
        let connection_manager: Arc<dyn ConnectionManager + Send + Sync> =
            Arc::new(ScriptedConnectionManager::new(vec![0, 0]));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let app = test_app();
        let socket_a = SocketId::new();
        let socket_b = SocketId::new();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket_a),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket_b),
                Some(default_retention()),
            )
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: app.id.clone(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
    }

    #[tokio::test]
    async fn presence_manager_suppresses_duplicate_authoritative_remove_reports() {
        let manager = PresenceManager::new();
        let connection_manager: Arc<dyn ConnectionManager + Send + Sync> =
            Arc::new(ScriptedConnectionManager::new(vec![0, 0, 0]));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let app = test_app();
        let socket = SocketId::new();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                Some(&socket),
                PresenceHistoryEventCause::Disconnect,
                None,
                0,
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                PresenceHistoryEventCause::OrphanCleanup,
                Some("dead-node"),
                0,
                Some(default_retention()),
            )
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: app.id.clone(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
        assert_eq!(page.items[1].cause, PresenceHistoryEventCause::Disconnect);
    }
}
