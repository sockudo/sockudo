use super::{CleanupConfig, ConnectionCleanupInfo, DisconnectTask, WebhookEvent};
use ahash::AHashMap;
use sockudo_adapter::channel_manager::ChannelManager;
use sockudo_adapter::connection_manager::ConnectionManager;
use sockudo_adapter::presence::global_presence_manager;
use sockudo_core::app::AppManager;
use sockudo_core::options::PresenceHistoryConfig;
use sockudo_core::presence_history::{PresenceHistoryEventCause, PresenceHistoryStore};
use sockudo_core::websocket::SocketId;
use sockudo_webhook::WebhookIntegration;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct CleanupWorker {
    connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    app_manager: Arc<dyn AppManager + Send + Sync>,
    webhook_integration: Option<Arc<WebhookIntegration>>,
    presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
    presence_history_config: PresenceHistoryConfig,
    config: CleanupConfig,
}

impl CleanupWorker {
    pub fn new(
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        webhook_integration: Option<Arc<WebhookIntegration>>,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_config: PresenceHistoryConfig,
        config: CleanupConfig,
    ) -> Self {
        Self {
            connection_manager,
            app_manager,
            webhook_integration,
            presence_history_store,
            presence_history_config,
            config,
        }
    }

    pub fn get_config(&self) -> &CleanupConfig {
        &self.config
    }

    /// Run the cleanup worker with optional cancellation support.
    pub async fn run(&self, receiver: super::CleanupReceiverHandle) {
        let cancel_token = CancellationToken::new();
        self.run_with_cancellation(receiver, cancel_token).await;
    }

    /// Run the cleanup worker with explicit cancellation support.
    pub async fn run_with_cancellation(
        &self,
        receiver: super::CleanupReceiverHandle,
        cancel_token: CancellationToken,
    ) {
        info!("Cleanup worker started with config: {:?}", self.config);

        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut last_batch_time = Instant::now();

        loop {
            tokio::select! {
                biased;

                _ = cancel_token.cancelled() => {
                    if !batch.is_empty() {
                        info!("Processing final batch of {} tasks before cancellation shutdown", batch.len());
                        self.process_batch(&mut batch).await;
                    }
                    info!("Cleanup worker shutting down due to cancellation");
                    break;
                }

                recv_result = timeout(
                    Duration::from_millis(self.config.batch_timeout_ms),
                    receiver.recv()
                ) => {
                    match recv_result {
                        Ok(Ok(task)) => {
                            debug!("Received disconnect task for socket: {}", task.socket_id);
                            batch.push(task);

                            if batch.len() >= self.config.batch_size
                                || last_batch_time.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms) {
                                self.process_batch(&mut batch).await;
                                last_batch_time = Instant::now();
                            }
                        }
                        Ok(Err(_)) => {
                            if !batch.is_empty() {
                                info!("Processing final batch of {} tasks before shutdown", batch.len());
                                self.process_batch(&mut batch).await;
                            }
                            info!("Cleanup worker shutting down (channel closed)");
                            break;
                        }
                        Err(_) => {
                            if !batch.is_empty() && last_batch_time.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms) {
                                debug!("Processing batch due to timeout: {} tasks", batch.len());
                                self.process_batch(&mut batch).await;
                                last_batch_time = Instant::now();
                            }
                        }
                    }
                }
            }
        }
    }

    async fn process_batch(&self, batch: &mut Vec<DisconnectTask>) {
        let batch_start = Instant::now();
        let batch_size = batch.len();

        debug!("Processing cleanup batch of {} tasks", batch_size);

        let mut channel_operations: AHashMap<(String, String), Vec<SocketId>> = AHashMap::new();
        let mut webhook_events = Vec::new();
        let mut connections_to_remove = Vec::new();

        for task in batch.iter() {
            for channel in &task.subscribed_channels {
                channel_operations
                    .entry((task.app_id.clone(), channel.clone()))
                    .or_default()
                    .push(task.socket_id);
            }

            connections_to_remove.push((task.socket_id, task.app_id.clone(), task.user_id.clone()));
        }

        self.batch_channel_cleanup(channel_operations).await;
        self.batch_connection_removal(connections_to_remove).await;

        for task in batch.iter() {
            webhook_events.extend(
                self.prepare_webhook_events(task, task.connection_info.as_ref())
                    .await,
            );
        }

        if !webhook_events.is_empty() {
            self.process_webhooks_async(webhook_events).await;
        }

        batch.clear();

        debug!("Completed cleanup batch in {:?}", batch_start.elapsed());
    }

    async fn batch_channel_cleanup(
        &self,
        channel_operations: AHashMap<(String, String), Vec<SocketId>>,
    ) {
        if channel_operations.is_empty() {
            return;
        }

        debug!(
            "Processing {} channel cleanup operations",
            channel_operations.len()
        );

        let mut total_success = 0;
        let mut total_errors = 0;

        let mut operations_by_app: AHashMap<String, Vec<(String, String, String)>> =
            AHashMap::new();

        for ((app_id, channel), socket_ids) in channel_operations {
            for socket_id in socket_ids {
                operations_by_app.entry(app_id.clone()).or_default().push((
                    socket_id.to_string(),
                    channel.clone(),
                    app_id.clone(),
                ));
            }
        }

        for (app_id, operations) in operations_by_app {
            debug!(
                "Processing batch unsubscribe for app {} with {} operations",
                app_id,
                operations.len()
            );

            match ChannelManager::batch_unsubscribe(&self.connection_manager, operations).await {
                Ok(results) => {
                    for (_channel_name, result) in results {
                        match result {
                            Ok(_) => total_success += 1,
                            Err(e) => {
                                total_errors += 1;
                                warn!(
                                    "Batch unsubscribe operation failed for app {}: {}",
                                    app_id, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Batch unsubscribe failed for app {}: {}", app_id, e);
                    total_errors += 1;
                }
            }
        }

        debug!(
            "Individual channel cleanup completed: {} successful, {} errors",
            total_success, total_errors
        );
    }

    async fn batch_connection_removal(&self, connections: Vec<(SocketId, String, Option<String>)>) {
        if connections.is_empty() {
            return;
        }

        debug!("Removing {} connections", connections.len());

        for (socket_id, app_id, user_id) in connections.into_iter() {
            let result = {
                let remove_result = self
                    .connection_manager
                    .remove_connection(&socket_id, &app_id)
                    .await;

                if let Some(uid) = &user_id
                    && let Err(e) = self
                        .connection_manager
                        .remove_user_socket(uid, &socket_id, &app_id)
                        .await
                {
                    warn!(
                        "Failed to remove user socket mapping for {}: {}",
                        socket_id, e
                    );
                }

                remove_result
            };

            if let Err(e) = result {
                warn!("Failed to remove connection {}: {}", socket_id, e);
            }
        }
    }

    async fn prepare_webhook_events(
        &self,
        task: &DisconnectTask,
        info: Option<&ConnectionCleanupInfo>,
    ) -> Vec<WebhookEvent> {
        let mut events = Vec::new();

        if let Some(info) = info
            && let Some(user_id) = &task.user_id
        {
            for channel in &info.presence_channels {
                events.push(WebhookEvent {
                    event_type: "member_removed".to_string(),
                    app_id: task.app_id.clone(),
                    channel: channel.clone(),
                    user_id: Some(user_id.clone()),
                    data: sonic_rs::json!({
                        "user_id": user_id,
                        "socket_id": task.socket_id.to_string()
                    }),
                });
            }
        }

        for channel in &task.subscribed_channels {
            let socket_count_info = self
                .connection_manager
                .get_channel_socket_count_info(&task.app_id, channel)
                .await;

            if socket_count_info.complete && socket_count_info.count == 0 {
                events.push(WebhookEvent {
                    event_type: "channel_vacated".to_string(),
                    app_id: task.app_id.clone(),
                    channel: channel.clone(),
                    user_id: None,
                    data: sonic_rs::json!({
                        "channel": channel
                    }),
                });
            }
        }

        events
    }

    async fn process_webhooks_async(&self, webhook_events: Vec<WebhookEvent>) {
        if let Some(webhook_integration) = &self.webhook_integration {
            debug!("Processing {} webhook events", webhook_events.len());

            let mut events_by_app: AHashMap<String, Vec<WebhookEvent>> = AHashMap::new();
            for event in webhook_events {
                events_by_app
                    .entry(event.app_id.clone())
                    .or_default()
                    .push(event);
            }

            let mut webhook_handles = Vec::new();

            for (app_id, events) in events_by_app {
                let webhook_integration = webhook_integration.clone();
                let app_manager = self.app_manager.clone();
                let connection_manager = self.connection_manager.clone();
                let presence_history_store = self.presence_history_store.clone();
                let presence_history_config = self.presence_history_config.clone();
                let handle = tokio::spawn(async move {
                    let app_config = match app_manager.find_by_id(&app_id).await {
                        Ok(Some(app)) => app,
                        Ok(None) => {
                            warn!("App {} not found for webhook events", app_id);
                            return;
                        }
                        Err(e) => {
                            error!("Failed to get app config for {}: {}", app_id, e);
                            return;
                        }
                    };

                    for event in events {
                        if let Err(e) = Self::send_webhook_event(
                            &connection_manager,
                            &webhook_integration,
                            &presence_history_store,
                            &presence_history_config,
                            &app_config,
                            &event,
                        )
                        .await
                        {
                            error!("Failed to send webhook event {}: {}", event.event_type, e);
                        }
                    }
                });

                webhook_handles.push(handle);
            }

            for handle in webhook_handles {
                if let Err(e) = handle.await {
                    error!("Webhook task panicked or was cancelled: {}", e);
                }
            }
        }
    }

    async fn send_webhook_event(
        connection_manager: &Arc<dyn ConnectionManager + Send + Sync>,
        webhook_integration: &Arc<WebhookIntegration>,
        presence_history_store: &Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_config: &PresenceHistoryConfig,
        app_config: &sockudo_core::app::App,
        event: &WebhookEvent,
    ) -> sockudo_core::error::Result<()> {
        debug!(
            "Sending {} webhook for app {} channel {}",
            event.event_type, app_config.id, event.channel
        );

        match event.event_type.as_str() {
            "member_removed" => {
                if let Some(user_id) = &event.user_id {
                    global_presence_manager()
                        .handle_member_removed(
                            connection_manager,
                            Arc::clone(presence_history_store),
                            app_config
                                .resolved_presence_history(&event.channel, presence_history_config)
                                .enabled,
                            Some(webhook_integration),
                            None,
                            app_config,
                            &event.channel,
                            user_id,
                            None,
                            PresenceHistoryEventCause::Disconnect,
                            None,
                            Some(
                                app_config
                                    .resolved_presence_history(
                                        &event.channel,
                                        presence_history_config,
                                    )
                                    .retention(),
                            ),
                        )
                        .await?;
                    debug!(
                        "Processed centralized member_removed for user {} in channel {}",
                        user_id, event.channel
                    );
                }
            }
            "channel_vacated" => {
                webhook_integration
                    .send_channel_vacated(app_config, &event.channel)
                    .await?;
                debug!("Sent channel_vacated webhook for channel {}", event.channel);
            }
            _ => {
                warn!("Unknown webhook event type: {}", event.event_type);
            }
        }

        Ok(())
    }
}
