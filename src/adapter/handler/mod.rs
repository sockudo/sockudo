// src/adapter/handler/mod.rs
pub mod authentication;
pub mod connection_management;
mod core;
pub mod message_handlers;
pub mod rate_limiting;
pub mod signin_management;
pub mod subscription_management;
pub mod timeout_management;
pub mod types;
pub mod validation;
pub mod webhook_management;

use crate::adapter::ConnectionManager;
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::cache::manager::CacheManager;
use crate::channel::ChannelManager;
use crate::error::{Error, Result};
use crate::metrics::MetricsInterface;
use crate::options::ServerOptions;
use crate::protocol::constants::CLIENT_EVENT_PREFIX;
use crate::protocol::messages::{MessageData, PusherMessage};
use crate::rate_limiter::RateLimiter;
use crate::watchlist::WatchlistManager;
use crate::webhook::integration::WebhookIntegration;
use crate::websocket::SocketId;

use crate::adapter::handler::types::{ClientEventRequest, SignInRequest, SubscriptionRequest};
use dashmap::DashMap;
use fastwebsockets::{FragmentCollectorRead, Frame, OpCode, WebSocketWrite, upgrade};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde_json::Value;
use std::sync::Arc;
use tokio::io::WriteHalf;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

pub struct ConnectionHandler {
    pub(crate) app_manager: Arc<dyn AppManager + Send + Sync>,
    pub(crate) channel_manager: Arc<RwLock<ChannelManager>>,
    pub(crate) connection_manager: Arc<Mutex<dyn ConnectionManager + Send + Sync>>,
    pub(crate) cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    pub(crate) metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
    webhook_integration: Option<Arc<WebhookIntegration>>,
    client_event_limiters: Arc<DashMap<SocketId, Arc<dyn RateLimiter + Send + Sync>>>,
    watchlist_manager: Arc<WatchlistManager>,
    server_options: Arc<ServerOptions>,
}

impl ConnectionHandler {
    pub fn new(
        app_manager: Arc<dyn AppManager + Send + Sync>,
        channel_manager: Arc<RwLock<ChannelManager>>,
        connection_manager: Arc<Mutex<dyn ConnectionManager + Send + Sync>>,
        cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
        metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
        webhook_integration: Option<Arc<WebhookIntegration>>,
        server_options: ServerOptions,
    ) -> Self {
        Self {
            app_manager,
            channel_manager,
            connection_manager,
            cache_manager,
            metrics,
            webhook_integration,
            client_event_limiters: Arc::new(DashMap::new()),
            watchlist_manager: Arc::new(WatchlistManager::new()),
            server_options: Arc::new(server_options),
        }
    }

    pub async fn handle_socket(&self, fut: upgrade::UpgradeFut, app_key: String) -> Result<()> {
        // Early validation and setup
        let app_config = self.validate_and_get_app(&app_key).await?;
        let (socket_rx, socket_tx) = self.upgrade_websocket(fut).await?;

        // Connection quota check
        self.check_connection_quota(&app_config).await?;

        // Initialize socket
        let socket_id = SocketId::new();
        self.initialize_socket(socket_id.clone(), socket_tx, &app_config)
            .await?;

        // Setup rate limiting if needed
        self.setup_rate_limiting(&socket_id, &app_config).await?;

        // Send connection established
        self.send_connection_established(&app_config.id, &socket_id)
            .await?;

        // Setup timeouts
        self.setup_initial_timeouts(&socket_id, &app_config).await?;

        // Main message loop
        let result = self
            .run_message_loop(socket_rx, &socket_id, &app_config)
            .await;

        // Cleanup
        self.cleanup_socket(&socket_id, &app_config).await;

        result
    }

    async fn initialize_socket(
        &self,
        socket_id: SocketId,
        socket_tx: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        app_config: &App,
    ) -> Result<()> {
        let mut connection_manager = self.connection_manager.lock().await;

        // Remove any existing connection with the same socket_id (unlikely but safe)
        if let Some(conn) = connection_manager
            .get_connection(&socket_id, &app_config.id)
            .await
        {
            connection_manager
                .cleanup_connection(&app_config.id, conn)
                .await;
        }

        // Add the new socket
        connection_manager
            .add_socket(
                socket_id.clone(),
                socket_tx,
                &app_config.id,
                &self.app_manager,
            )
            .await?;

        // Update metrics
        if let Some(ref metrics) = self.metrics {
            let metrics_locked = metrics.lock().await;
            metrics_locked.mark_new_connection(&app_config.id, &socket_id);
        }

        Ok(())
    }

    async fn validate_and_get_app(&self, app_key: &str) -> Result<App> {
        match self.app_manager.find_by_key(app_key).await {
            Ok(Some(app)) if app.enabled => Ok(app),
            Ok(Some(_)) => Err(Error::ApplicationDisabled),
            Ok(None) => Err(Error::ApplicationNotFound),
            Err(e) => {
                error!(
                    "Database error during app lookup for key {}: {}",
                    app_key, e
                );
                Err(Error::Internal("App lookup failed".to_string()))
            }
        }
    }

    async fn upgrade_websocket(
        &self,
        fut: upgrade::UpgradeFut,
    ) -> Result<(
        FragmentCollectorRead<tokio::io::ReadHalf<TokioIo<Upgraded>>>,
        WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
    )> {
        let ws = fut.await.map_err(Error::WebSocket)?;
        let (rx, tx) = ws.split(tokio::io::split);
        Ok((FragmentCollectorRead::new(rx), tx))
    }

    async fn check_connection_quota(&self, app_config: &App) -> Result<()> {
        if app_config.max_connections == 0 {
            return Ok(());
        }

        let current_count = self
            .connection_manager
            .lock()
            .await
            .get_sockets_count(&app_config.id)
            .await
            .map_err(|e| {
                error!(
                    "Error getting sockets count for app {}: {}",
                    app_config.id, e
                );
                Error::Internal("Failed to check connection quota".to_string())
            })?;

        if current_count >= app_config.max_connections as usize {
            return Err(Error::OverConnectionQuota);
        }

        Ok(())
    }

    async fn run_message_loop(
        &self,
        mut fragment_collector: FragmentCollectorRead<tokio::io::ReadHalf<TokioIo<Upgraded>>>,
        socket_id: &SocketId,
        app_config: &App,
    ) -> Result<()> {
        while let Ok(frame) = fragment_collector
            .read_frame(&mut |_| async { Ok::<_, fastwebsockets::WebSocketError>(()) })
            .await
        {
            match frame.opcode {
                OpCode::Close => {
                    info!("Received Close frame from socket {}", socket_id);
                    self.handle_disconnect(&app_config.id, socket_id).await?;
                    break;
                }
                OpCode::Text | OpCode::Binary => {
                    if let Err(e) = self
                        .handle_message(frame, socket_id, app_config.clone())
                        .await
                    {
                        error!("Message handling error for socket {}: {}", socket_id, e);
                        if e.is_fatal() {
                            self.handle_fatal_error(socket_id, app_config, &e).await?;
                            break;
                        }
                    }
                }
                OpCode::Ping => {
                    self.handle_ping_frame(socket_id, app_config).await?;
                }
                _ => {
                    warn!("Unsupported opcode from {}: {:?}", socket_id, frame.opcode);
                }
            }
        }

        Ok(())
    }

    async fn handle_message(
        &self,
        frame: Frame<'static>,
        socket_id: &SocketId,
        app_config: App,
    ) -> Result<()> {
        // Update activity timeout
        self.update_activity_timeout(&app_config.id, socket_id)
            .await?;

        // Parse message
        let message = self.parse_message(&frame)?;
        let event_name = message
            .event
            .as_deref()
            .ok_or_else(|| Error::InvalidEventName("Event name is required".into()))?;

        info!(
            "Received message from {}: event '{}'",
            socket_id, event_name
        );

        // Handle rate limiting for client events
        if event_name.starts_with(CLIENT_EVENT_PREFIX) {
            self.check_client_event_rate_limit(socket_id, &app_config, event_name)
                .await?;
        }

        // Route message to appropriate handler
        match event_name {
            "pusher:ping" => self.handle_ping(&app_config.id, socket_id).await,
            "pusher:subscribe" => {
                let request = SubscriptionRequest::from_message(&message)?;
                self.handle_subscribe_request(socket_id, &app_config, request)
                    .await
            }
            "pusher:unsubscribe" => {
                self.handle_unsubscribe(socket_id, &message, &app_config)
                    .await
            }
            "pusher:signin" => {
                let request = SignInRequest::from_message(&message)?;
                self.handle_signin_request(socket_id, &app_config, request)
                    .await
            }
            "pusher:pong" => self.handle_pong(&*app_config.id, socket_id).await,
            _ if event_name.starts_with(CLIENT_EVENT_PREFIX) => {
                let request = self.parse_client_event(&message)?;
                self.handle_client_event_request(socket_id, &app_config, request)
                    .await
            }
            _ => {
                warn!("Unknown event '{}' from socket {}", event_name, socket_id);
                Ok(()) // Ignore unknown events per Pusher spec
            }
        }
    }

    fn parse_message(&self, frame: &Frame<'static>) -> Result<PusherMessage> {
        let payload = String::from_utf8(frame.payload.to_vec())
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid UTF-8: {}", e)))?;

        serde_json::from_str(&payload)
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid JSON: {}", e)))
    }

    fn parse_client_event(&self, message: &PusherMessage) -> Result<ClientEventRequest> {
        let event = message
            .event
            .as_ref()
            .ok_or_else(|| Error::InvalidEventName("Event name required".into()))?
            .clone();

        let channel = message
            .channel
            .as_ref()
            .ok_or_else(|| Error::ClientEvent("Channel required for client event".into()))?
            .clone();

        let data = match &message.data {
            Some(MessageData::Json(data)) => data.clone(),
            Some(MessageData::String(s)) => {
                serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone()))
            }
            _ => Value::Null,
        };

        Ok(ClientEventRequest {
            event,
            channel,
            data,
        })
    }

    async fn handle_fatal_error(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        error: &Error,
    ) -> Result<()> {
        // Send error message
        self.send_error(&app_config.id, socket_id, error, None)
            .await
            .unwrap_or_else(|e| {
                error!("Failed to send error to socket {}: {}", socket_id, e);
            });

        // Close connection
        self.close_connection(
            socket_id,
            app_config,
            error.close_code(),
            &error.to_string(),
        )
        .await?;

        // Handle disconnect cleanup
        self.handle_disconnect(&app_config.id, socket_id).await?;

        Ok(())
    }

    async fn cleanup_socket(&self, socket_id: &SocketId, app_config: &App) {
        // Remove rate limiter
        self.client_event_limiters.remove(socket_id);

        // Clear timeouts
        if let Err(e) = self.clear_activity_timeout(&app_config.id, socket_id).await {
            warn!("Failed to clear activity timeout for {}: {}", socket_id, e);
        }

        if let Err(e) = self
            .clear_user_authentication_timeout(&app_config.id, socket_id)
            .await
        {
            warn!("Failed to clear auth timeout for {}: {}", socket_id, e);
        }

        info!("Socket {} cleanup completed", socket_id);
    }
}
