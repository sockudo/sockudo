use super::{
    CleanupConfig, ConnectionCleanupInfo, DisconnectTask, PresenceCleanupContext, WebhookEvent,
};
use ahash::AHashMap;
use sockudo_adapter::channel_manager::ChannelManager;
use sockudo_adapter::connection_manager::ConnectionManager;
use sockudo_core::app::AppManager;
use sockudo_core::channel::ChannelType;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::presence_history::PresenceHistoryEventCause;
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
    presence: PresenceCleanupContext,
    config: CleanupConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
}

impl CleanupWorker {
    pub fn new(
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        webhook_integration: Option<Arc<WebhookIntegration>>,
        presence: PresenceCleanupContext,
        config: CleanupConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Self {
        Self {
            connection_manager,
            app_manager,
            webhook_integration,
            presence,
            config,
            metrics,
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
        info!(
            batch_size = self.config.batch_size,
            batch_timeout_ms = self.config.batch_timeout_ms,
            "cleanup worker started"
        );

        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut last_batch_time = Instant::now();

        loop {
            tokio::select! {
                biased;

                _ = cancel_token.cancelled() => {
                    if !batch.is_empty() {
                        info!(task_count = batch.len(), shutdown_reason = "cancelled", "processing final cleanup batch");
                        self.process_batch(&mut batch).await;
                    }
                    info!(shutdown_reason = "cancelled", "cleanup worker stopping");
                    break;
                }

                recv_result = timeout(
                    Duration::from_millis(self.config.batch_timeout_ms),
                    receiver.recv()
                ) => {
                    match recv_result {
                        Ok(Ok(task)) => {
                            debug!(socket_id = %task.socket_id, "disconnect cleanup task received");
                            batch.push(task);

                            if batch.len() >= self.config.batch_size
                                || last_batch_time.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms) {
                                self.process_batch(&mut batch).await;
                                last_batch_time = Instant::now();
                            }
                        }
                        Ok(Err(_)) => {
                            if !batch.is_empty() {
                                info!(task_count = batch.len(), shutdown_reason = "channel_closed", "processing final cleanup batch");
                                self.process_batch(&mut batch).await;
                            }
                            info!(shutdown_reason = "channel_closed", "cleanup worker stopping");
                            break;
                        }
                        Err(_) => {
                            if !batch.is_empty() && last_batch_time.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms) {
                                debug!(task_count = batch.len(), trigger = "timeout", "processing cleanup batch");
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

        debug!(task_count = batch_size, "processing cleanup batch");

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
            self.process_cleanup_events(webhook_events).await;
        }

        batch.clear();

        debug!(
            task_count = batch_size,
            elapsed_ms = batch_start.elapsed().as_millis(),
            "cleanup batch completed"
        );
    }

    async fn batch_channel_cleanup(
        &self,
        channel_operations: AHashMap<(String, String), Vec<SocketId>>,
    ) {
        if channel_operations.is_empty() {
            return;
        }

        debug!(
            operation_count = channel_operations.len(),
            "processing channel cleanup operations"
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
                app_id = %app_id,
                operation_count = operations.len(),
                "processing batch unsubscribe"
            );

            match ChannelManager::batch_unsubscribe(&self.connection_manager, operations).await {
                Ok(results) => {
                    for (channel_name, result) in &results {
                        match result {
                            Ok((_was_removed, _remaining_connections, local_vacated)) => {
                                total_success += 1;
                                // Per-pod gauge: decrement when the channel empties on this node.
                                if *local_vacated && let Some(ref metrics) = self.metrics {
                                    let channel_type =
                                        ChannelType::from_name(channel_name).as_str();
                                    metrics.mark_channel_deactivated(&app_id, channel_type);
                                }
                            }
                            Err(e) => {
                                total_errors += 1;
                                warn!(app_id = %app_id, error = %e, "batch unsubscribe operation failed");
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(app_id = %app_id, error = %e, "batch unsubscribe failed");
                    total_errors += 1;
                }
            }
        }

        debug!(
            success_count = total_success,
            error_count = total_errors,
            "individual channel cleanup completed"
        );
    }

    async fn batch_connection_removal(&self, connections: Vec<(SocketId, String, Option<String>)>) {
        if connections.is_empty() {
            return;
        }

        debug!(connection_count = connections.len(), "removing connections");

        for (socket_id, app_id, _user_id) in connections {
            // Comprehensive: handles shutdown, user-socket mappings, channels, and presence.
            let result = self
                .connection_manager
                .remove_connection(&socket_id, &app_id)
                .await;

            if let Err(e) = result {
                warn!(
                    app_id = %app_id,
                    socket_id = %socket_id,
                    error = %e,
                    "connection removal failed"
                );
            }
        }
    }

    async fn prepare_webhook_events(
        &self,
        task: &DisconnectTask,
        info: Option<&ConnectionCleanupInfo>,
    ) -> Vec<WebhookEvent> {
        let mut events = Vec::new();

        if let Some(info) = info {
            for channel in &info.presence_channels {
                // Socket authentication and presence membership can use different IDs.
                let Some(member) = info.presence_members.get(channel) else {
                    continue;
                };
                events.push(WebhookEvent {
                    event_type: "member_removed".to_string(),
                    app_id: task.app_id.clone(),
                    channel: channel.clone(),
                    user_id: Some(member.user_id.clone()),
                    socket_id: Some(task.socket_id),
                    data: sonic_rs::json!({
                        "user_id": member.user_id,
                        "socket_id": task.socket_id.to_string()
                    }),
                    presence_user_info: member.user_info.clone(),
                    presence_ungraceful_timeout_seconds: task.presence_ungraceful_timeout_seconds,
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
                    socket_id: None,
                    data: sonic_rs::json!({
                        "channel": channel
                    }),
                    presence_user_info: None,
                    presence_ungraceful_timeout_seconds: 0,
                });
            }
        }

        events
    }

    async fn process_cleanup_events(&self, cleanup_events: Vec<WebhookEvent>) {
        debug!(
            event_count = cleanup_events.len(),
            "processing disconnect cleanup events"
        );

        let mut events_by_app: AHashMap<String, Vec<WebhookEvent>> = AHashMap::new();
        for event in cleanup_events {
            events_by_app
                .entry(event.app_id.clone())
                .or_default()
                .push(event);
        }

        let mut cleanup_handles = Vec::new();

        for (app_id, events) in events_by_app {
            let webhook_integration = self.webhook_integration.clone();
            let app_manager = self.app_manager.clone();
            let connection_manager = self.connection_manager.clone();
            let presence_history_store = Arc::clone(&self.presence.history_store);
            let presence_history_config = self.presence.history_config.clone();
            let presence_manager = Arc::clone(&self.presence.manager);
            let handle = tokio::spawn(async move {
                let app_config = match app_manager.find_by_id(&app_id).await {
                    Ok(Some(app)) => app,
                    Ok(None) => {
                        warn!(app_id = %app_id, "app not found for disconnect cleanup events");
                        return;
                    }
                    Err(e) => {
                        error!(app_id = %app_id, error = %e, "app lookup failed for disconnect cleanup events");
                        return;
                    }
                };

                for event in events {
                    if let Err(e) = Self::process_cleanup_event(
                        &connection_manager,
                        &presence_manager,
                        webhook_integration.as_ref(),
                        &presence_history_store,
                        &presence_history_config,
                        &app_config,
                        &event,
                    )
                    .await
                    {
                        error!(
                            app_id = %app_id,
                            event = %event.event_type,
                            error = %e,
                            "disconnect cleanup event processing failed"
                        );
                    }
                }
            });

            cleanup_handles.push(handle);
        }

        for handle in cleanup_handles {
            if let Err(e) = handle.await {
                error!(error = %e, "disconnect cleanup task failed");
            }
        }
    }

    async fn process_cleanup_event(
        connection_manager: &Arc<dyn ConnectionManager + Send + Sync>,
        presence_manager: &sockudo_adapter::presence::PresenceManager,
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        presence_history_store: &Arc<
            dyn sockudo_core::presence_history::PresenceHistoryStore + Send + Sync,
        >,
        presence_history_config: &sockudo_core::options::PresenceHistoryConfig,
        app_config: &sockudo_core::app::App,
        event: &WebhookEvent,
    ) -> sockudo_core::error::Result<()> {
        debug!(
            app_id = %app_config.id,
            channel = %event.channel,
            event = %event.event_type,
            "processing disconnect cleanup event"
        );

        match event.event_type.as_str() {
            "member_removed" => {
                if let Some(user_id) = &event.user_id {
                    presence_manager
                        .handle_member_removed_with_info(
                            connection_manager,
                            Arc::clone(presence_history_store),
                            app_config
                                .resolved_presence_history(&event.channel, presence_history_config)
                                .enabled,
                            webhook_integration,
                            None,
                            app_config,
                            &event.channel,
                            user_id,
                            event.socket_id.as_ref(),
                            PresenceHistoryEventCause::Disconnect,
                            None,
                            event.presence_user_info.clone(),
                            event.presence_ungraceful_timeout_seconds,
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
                        app_id = %app_config.id,
                        channel = %event.channel,
                        user_id = %user_id,
                        "centralized member removal processed"
                    );
                }
            }
            "channel_vacated" => {
                if let Some(webhook_integration) = webhook_integration {
                    webhook_integration
                        .send_channel_vacated(app_config, &event.channel)
                        .await?;
                    debug!(
                        app_id = %app_config.id,
                        channel = %event.channel,
                        event = "channel_vacated",
                        "cleanup webhook sent"
                    );
                }
            }
            _ => {
                warn!(event = %event.event_type, "unknown cleanup webhook event type");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossfire::mpsc;
    use futures_util::StreamExt;
    use sockudo_adapter::cleanup::{CleanupSender, DisconnectTask};
    use sockudo_adapter::handler::ConnectionHandler;
    use sockudo_adapter::local_adapter::LocalAdapter;
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_cache::MemoryCacheManager;
    use sockudo_core::app::{App, AppManager, AppPolicy};
    use sockudo_core::channel::PresenceMemberInfo;
    use sockudo_core::options::{MemoryCacheOptions, PresenceHistoryConfig, ServerOptions};
    use sockudo_core::presence_history::NoopPresenceHistoryStore;
    use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
    use sockudo_protocol::{ProtocolVersion, WireFormat};
    use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
    use sockudo_ws::client::WebSocketClient;
    use sockudo_ws::{
        Config as WsConfig, Http1, Message as WsMessage, Stream as WsStream, WebSocketStream,
    };
    use sonic_rs::JsonValueTrait;
    use std::time::{Duration, Instant};
    use tokio::net::{TcpListener, TcpStream};

    const APP_ID: &str = "cleanup-worker-test";

    fn make_app() -> App {
        App::from_policy(
            APP_ID.to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            true,
            AppPolicy::default(),
        )
    }

    async fn make_ws_pair() -> (WebSocketWriter, WebSocketStream<WsStream<Http1>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            sockudo_ws::handshake::server_handshake(&mut stream)
                .await
                .unwrap();
            let ws = WebSocket::from_tcp(stream, WsConfig::default());
            let (mut reader, writer) = ws.split();
            tokio::spawn(async move {
                while let Some(result) = reader.next().await {
                    if result.is_err() {
                        break;
                    }
                }
            });
            writer
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let client = WebSocketClient::<Http1>::new(WsConfig::default());
        let (client_ws, _): (WebSocketStream<WsStream<Http1>>, _) = client
            .connect(client_stream, &addr.to_string(), "/", None)
            .await
            .unwrap();

        let server_writer = server_task.await.unwrap();
        (server_writer, client_ws)
    }

    fn make_worker(adapter: Arc<dyn ConnectionManager + Send + Sync>) -> CleanupWorker {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        make_worker_with_app_manager(adapter, app_manager)
    }

    fn make_worker_with_app_manager(
        adapter: Arc<dyn ConnectionManager + Send + Sync>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
    ) -> CleanupWorker {
        CleanupWorker::new(
            adapter,
            app_manager,
            None,
            PresenceCleanupContext {
                history_store: Arc::new(NoopPresenceHistoryStore),
                history_config: PresenceHistoryConfig::default(),
                manager: Arc::new(sockudo_adapter::presence::PresenceManager::new()),
            },
            CleanupConfig {
                batch_size: 64,
                batch_timeout_ms: 50,
                ..Default::default()
            },
            None,
        )
    }

    #[tokio::test]
    async fn cleanup_events_use_channel_presence_identity() {
        let adapter = Arc::new(LocalAdapter::new());
        adapter.init().await;
        let worker = make_worker(adapter);
        let memberships = [
            ("presence-pipeline-3", "anna:tab1"),
            ("presence-channel-111", "anna"),
        ];
        for user_id in [Some("signed-in-anna".to_string()), None] {
            let info = ConnectionCleanupInfo {
                presence_channels: vec![
                    "presence-pipeline-3".to_string(),
                    "presence-channel-111".to_string(),
                    "presence-missing".to_string(),
                ],
                presence_members: memberships
                    .iter()
                    .map(|(channel, user_id)| {
                        (
                            channel.to_string(),
                            PresenceMemberInfo {
                                user_id: user_id.to_string(),
                                user_info: Some(sonic_rs::json!({"name": user_id})),
                            },
                        )
                    })
                    .collect(),
                auth_info: None,
            };
            let task = DisconnectTask {
                socket_id: SocketId::new(),
                app_id: APP_ID.to_string(),
                subscribed_channels: info.presence_channels.clone(),
                user_id,
                cause: sockudo_core::websocket::DisconnectCause::TransportError,
                timestamp: Instant::now(),
                connection_info: Some(info),
                presence_ungraceful_timeout_seconds: 1,
            };
            let events = worker
                .prepare_webhook_events(&task, task.connection_info.as_ref())
                .await;
            let removed: Vec<_> = events
                .iter()
                .filter(|event| event.event_type == "member_removed")
                .collect();
            assert_eq!(
                removed.len(),
                memberships.len(),
                "missing channel membership must not invent a leave identity"
            );
            for (event, (channel, user_id)) in removed.iter().zip(memberships) {
                assert_eq!(event.channel, channel);
                assert_eq!(event.user_id.as_deref(), Some(user_id));
                assert_eq!(event.data["user_id"].as_str(), Some(user_id));
                assert_eq!(
                    event.presence_user_info.as_ref().unwrap()["name"].as_str(),
                    Some(user_id)
                );
                assert_eq!(event.presence_ungraceful_timeout_seconds, 1);
            }
        }
    }

    #[tokio::test]
    async fn disconnect_cleanup_retains_then_emits_presence_leave_without_webhooks() {
        let adapter = Arc::new(LocalAdapter::new());
        adapter.init().await;

        let app_manager = Arc::new(MemoryAppManager::new());
        app_manager.create_app(make_app()).await.unwrap();

        let observer_socket = SocketId::new();
        let (observer_writer, mut observer_client) = make_ws_pair().await;
        adapter
            .add_socket(
                observer_socket,
                observer_writer,
                APP_ID,
                app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V1,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
            )
            .await
            .unwrap();
        adapter
            .add_to_channel(APP_ID, "presence-room", &observer_socket)
            .await
            .unwrap();

        let leaving_socket = SocketId::new();
        let (leaving_writer, _leaving_client) = make_ws_pair().await;
        adapter
            .add_socket(
                leaving_socket,
                leaving_writer,
                APP_ID,
                app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V1,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
            )
            .await
            .unwrap();
        adapter
            .add_to_channel(APP_ID, "presence-room", &leaving_socket)
            .await
            .unwrap();

        let leaving_connection = adapter
            .get_connection(&leaving_socket, APP_ID)
            .await
            .unwrap();
        {
            let mut guard = leaving_connection.inner.lock().await;
            guard.state.user_id = Some("signed-in-user".to_string());
        }
        adapter.add_user(leaving_connection).await.unwrap();

        let namespace = adapter.get_namespace(APP_ID).await.unwrap();
        namespace
            .presence_data
            .entry(leaving_socket)
            .or_default()
            .insert(
                "presence-room".to_string(),
                PresenceMemberInfo {
                    user_id: "leaving-user".to_string(),
                    user_info: Some(sonic_rs::json!({"status": "busy"})),
                },
            );

        let mut presence_members = std::collections::HashMap::new();
        presence_members.insert(
            "presence-room".to_string(),
            PresenceMemberInfo {
                user_id: "leaving-user".to_string(),
                user_info: Some(sonic_rs::json!({"status": "busy"})),
            },
        );
        let task = DisconnectTask {
            socket_id: leaving_socket,
            app_id: APP_ID.to_string(),
            subscribed_channels: vec!["presence-room".to_string()],
            user_id: Some("signed-in-user".to_string()),
            cause: sockudo_core::websocket::DisconnectCause::TransportError,
            timestamp: Instant::now(),
            connection_info: Some(ConnectionCleanupInfo {
                presence_channels: vec!["presence-room".to_string()],
                presence_members,
                auth_info: None,
            }),
            presence_ungraceful_timeout_seconds: 1,
        };

        let worker = make_worker_with_app_manager(
            adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
            app_manager as Arc<dyn AppManager + Send + Sync>,
        );
        let mut batch = vec![task];
        worker.process_batch(&mut batch).await;

        let retained = adapter
            .get_channel_members(APP_ID, "presence-room")
            .await
            .unwrap();
        assert_eq!(
            retained
                .get("leaving-user")
                .and_then(|member| member.user_info.as_ref())
                .and_then(|info| info.get("status"))
                .and_then(sonic_rs::Value::as_str),
            Some("busy")
        );

        let frame = tokio::time::timeout(Duration::from_secs(2), observer_client.next())
            .await
            .expect("observer must receive member_removed")
            .expect("observer websocket must remain open")
            .expect("observer frame must be readable");
        let message: sockudo_protocol::messages::PusherMessage = match frame {
            WsMessage::Text(bytes) => sonic_rs::from_slice(bytes.as_ref()).unwrap(),
            WsMessage::Binary(bytes) => sonic_rs::from_slice(&bytes).unwrap(),
            other => panic!("unexpected websocket frame: {other:?}"),
        };

        assert_eq!(
            message.event.as_deref(),
            Some("pusher_internal:member_removed")
        );
        assert_eq!(message.channel.as_deref(), Some("presence-room"));
        assert!(
            message
                .data
                .as_ref()
                .is_some_and(|data| sonic_rs::to_string(data).unwrap().contains("leaving-user"))
        );
    }

    #[tokio::test]
    async fn v2_ungraceful_disconnect_uses_recovery_presence_lease() {
        let adapter = Arc::new(LocalAdapter::new());
        adapter.init().await;

        let app_manager = Arc::new(MemoryAppManager::new());
        app_manager.create_app(make_app()).await.unwrap();

        let socket_id = SocketId::new();
        let (writer, _client) = make_ws_pair().await;
        adapter
            .add_socket(
                socket_id,
                writer,
                APP_ID,
                app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V2,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
            )
            .await
            .unwrap();

        let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(8);
        let cache = Arc::new(MemoryCacheManager::new(
            "cleanup-worker-v2-presence-test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let handler = ConnectionHandler::builder(
            app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
            adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
            cache,
            ServerOptions::default(),
        )
        .local_adapter(adapter.clone())
        .cleanup_queue(CleanupSender::Direct(tx))
        .build();

        handler
            .handle_ungraceful_disconnect(APP_ID, &socket_id)
            .await
            .unwrap();
        let task = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("disconnect task must be queued")
            .expect("cleanup queue must remain open");

        assert_eq!(task.presence_ungraceful_timeout_seconds, 15);

        let worker = make_worker_with_app_manager(
            adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
            app_manager as Arc<dyn AppManager + Send + Sync>,
        );
        let mut batch = vec![task];
        worker.process_batch(&mut batch).await;
    }

    #[tokio::test]
    async fn deferred_worker_completes_exhaustive_cleanup() {
        // Arrange: socket with user identity and two channels
        let adapter = Arc::new(LocalAdapter::new());
        adapter.init().await;

        let app_manager = Arc::new(MemoryAppManager::new());
        app_manager.create_app(make_app()).await.unwrap();

        let socket_id = SocketId::new();
        let (writer, _client) = make_ws_pair().await;
        adapter
            .add_socket(
                socket_id,
                writer,
                APP_ID,
                app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V1,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
            )
            .await
            .unwrap();

        // Associate the same socket with two identities, as happens when a
        // presence-derived identity is replaced by signin metadata.
        let ws_ref = adapter.get_connection(&socket_id, APP_ID).await.unwrap();
        {
            let mut guard = ws_ref.inner.lock().await;
            guard.set_user_info(sockudo_core::websocket::UserInfo {
                id: "user-alpha".to_string(),
                watchlist: None,
                info: None,
                capabilities: None,
                meta: None,
            });
        }
        adapter.add_user(ws_ref.clone()).await.unwrap();
        {
            let mut guard = ws_ref.inner.lock().await;
            guard.state.user_id = Some("user-beta".to_string());
        }
        adapter.add_user(ws_ref.clone()).await.unwrap();

        // Subscribe to two channels
        adapter
            .add_to_channel(APP_ID, "public-feed", &socket_id)
            .await
            .unwrap();
        adapter
            .add_to_channel(APP_ID, "presence-room", &socket_id)
            .await
            .unwrap();

        // Confirm indexes are populated
        assert!(adapter.get_connection(&socket_id, APP_ID).await.is_some());
        assert!(
            adapter
                .is_in_channel(APP_ID, "public-feed", &socket_id)
                .await
                .unwrap()
        );
        assert!(
            adapter
                .is_in_channel(APP_ID, "presence-room", &socket_id)
                .await
                .unwrap()
        );
        let user_sockets = adapter
            .get_user_sockets("user-alpha", APP_ID)
            .await
            .unwrap();
        assert!(!user_sockets.is_empty());
        assert_eq!(
            adapter
                .get_user_sockets("user-beta", APP_ID)
                .await
                .unwrap()
                .len(),
            1
        );

        let namespace = adapter.get_namespace(APP_ID).await.unwrap();
        namespace
            .presence_data
            .entry(socket_id)
            .or_default()
            .insert(
                "presence-room".to_string(),
                PresenceMemberInfo {
                    user_id: "user-beta".to_string(),
                    user_info: None,
                },
            );

        // The real async handler must cancel before the task is consumed.
        let token = ws_ref.cancellation_token();
        let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(8);
        let cache = Arc::new(MemoryCacheManager::new(
            "cleanup-worker-test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let handler = ConnectionHandler::builder(
            app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
            adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
            cache,
            ServerOptions::default(),
        )
        .local_adapter(adapter.clone())
        .cleanup_queue(CleanupSender::Direct(tx))
        .build();

        handler.handle_disconnect(APP_ID, &socket_id).await.unwrap();
        assert!(token.is_cancelled());

        let task = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("disconnect task must be queued")
            .expect("cleanup queue must remain open");
        let worker = make_worker(adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>);
        let mut batch = vec![task];
        worker.process_batch(&mut batch).await;

        // Assert: all indexes cleaned by the comprehensive remove_connection path
        assert!(
            adapter.get_connection(&socket_id, APP_ID).await.is_none(),
            "connection must be removed"
        );
        assert!(
            !adapter
                .is_in_channel(APP_ID, "public-feed", &socket_id)
                .await
                .unwrap(),
            "socket must be removed from public-feed"
        );
        assert!(
            !adapter
                .is_in_channel(APP_ID, "presence-room", &socket_id)
                .await
                .unwrap(),
            "socket must be removed from presence-room"
        );
        let user_sockets = adapter
            .get_user_sockets("user-alpha", APP_ID)
            .await
            .unwrap();
        assert!(
            user_sockets.is_empty(),
            "first user socket mapping must be empty after comprehensive removal"
        );
        assert!(
            adapter
                .get_user_sockets("user-beta", APP_ID)
                .await
                .unwrap()
                .is_empty(),
            "second user socket mapping must be empty after comprehensive removal"
        );
        assert!(
            namespace.presence_data.get(&socket_id).is_none(),
            "presence data must be removed by comprehensive cleanup"
        );
    }

    #[tokio::test]
    async fn stale_task_after_sync_fallback_is_harmless() {
        // Arrange: socket already removed (sync fallback ran first)
        let adapter = Arc::new(LocalAdapter::new());
        adapter.init().await;

        let app_manager = Arc::new(MemoryAppManager::new());
        app_manager.create_app(make_app()).await.unwrap();

        let socket_id = SocketId::new();
        let (writer, _client) = make_ws_pair().await;
        adapter
            .add_socket(
                socket_id,
                writer,
                APP_ID,
                app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V1,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
            )
            .await
            .unwrap();

        // Remove connection synchronously (simulating the fallback path ran first)
        adapter.remove_connection(&socket_id, APP_ID).await.unwrap();
        assert!(adapter.get_connection(&socket_id, APP_ID).await.is_none());

        // Build a stale task referencing the already-removed socket
        let stale_task = DisconnectTask {
            socket_id,
            app_id: APP_ID.to_string(),
            subscribed_channels: vec!["public-channel".to_string()],
            user_id: Some("stale-user".to_string()),
            cause: sockudo_core::websocket::DisconnectCause::Unknown,
            timestamp: Instant::now(),
            connection_info: None,
            presence_ungraceful_timeout_seconds: 0,
        };

        let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(8);
        tx.try_send(stale_task).unwrap();
        drop(tx);

        let worker = make_worker(adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>);

        // Act: worker processes the stale task — must not panic
        tokio::time::timeout(Duration::from_secs(5), worker.run(rx))
            .await
            .expect("worker must not hang on stale tasks");

        // Assert: no entries recreated
        assert!(
            adapter.get_connection(&socket_id, APP_ID).await.is_none(),
            "stale task must not recreate connection"
        );
        let sockets_count = adapter.get_sockets_count(APP_ID).await.unwrap();
        assert_eq!(
            sockets_count, 0,
            "no sockets should exist after stale task processing"
        );
    }
}
