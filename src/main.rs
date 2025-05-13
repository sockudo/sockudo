mod adapter;
mod app;
mod cache;
mod channel;
mod error;
mod http_handler;
pub mod log;
mod metrics;
mod namespace;
mod options;
mod protocol;
mod queue;
mod rate_limiter;
mod token;
pub mod utils;
mod webhook;
mod websocket;
mod ws_handler;

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::Method;
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{serve, Router};
use error::Error;
use serde_json::{from_str, json, Value};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{Mutex, RwLock};

use crate::adapter::local_adapter::LocalAdapter;
use crate::adapter::redis_adapter::{RedisAdapter, RedisAdapterConfig};
use crate::adapter::Adapter;
use crate::adapter::ConnectionHandler;
use crate::app::auth::AuthValidator;
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::app::memory_app_manager::MemoryAppManager;
use crate::cache::manager::CacheManager;
use crate::cache::redis_cache_manager::{RedisCacheConfig, RedisCacheManager};
use crate::channel::ChannelManager;
use crate::error::Result;
use crate::http_handler::{
    batch_events, channel, channel_users, channels, events, metrics, terminate_user_connections,
    up, usage,
};
use crate::log::Log;
use crate::metrics::{MetricsFactory, MetricsInterface};
use crate::options::{ServerOptions, WebhooksConfig};
use crate::webhook::integration::{BatchingConfig, WebhookConfig, WebhookIntegration};
use crate::webhook::types::Webhook;
use crate::ws_handler::handle_ws_upgrade;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Server state containing all managers
#[derive(Clone)]
struct ServerState {
    app_manager: Arc<dyn AppManager + Send + Sync>,
    channel_manager: Arc<RwLock<ChannelManager>>,
    connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>,
    auth_validator: Arc<AuthValidator>,
    cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    webhooks_integration: Arc<WebhookIntegration>,
    metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
    running: Arc<AtomicBool>,
}

/// Main server struct
struct SockudoServer {
    config: ServerOptions,
    state: ServerState,
    handler: Arc<ConnectionHandler>,
}

impl SockudoServer {
    /// Get HTTP address from ServerOptions
    fn get_http_addr(&self) -> SocketAddr {
        format!("{}:{}", self.config.host, self.config.port)
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:6001".parse().unwrap())
    }

    /// Get metrics address from ServerOptions
    fn get_metrics_addr(&self) -> SocketAddr {
        format!("{}:{}", self.config.metrics.host, self.config.metrics.port)
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:9601".parse().unwrap())
    }

    /// Create a new server instance
    async fn new(config: ServerOptions) -> Result<Self> {
        // Initialize app manager
        let app_manager = Arc::new(MemoryAppManager::new());

        // Get Redis URL from adapter config or fallback
        let redis_url = match config.adapter.driver.as_str() {
            "redis" => {
                if let Some(url) = config.adapter.redis.redis_pub_options.get("url") {
                    url.as_str().unwrap_or("redis://127.0.0.1:6379").to_string()
                } else {
                    "redis://127.0.0.1:6379".to_string()
                }
            }
            _ => "redis://127.0.0.1:6379".to_string(),
        };

        // Initialize Redis adapter with configuration
        let adapter_config = RedisAdapterConfig {
            url: redis_url.clone(),
            prefix: config.adapter.redis.prefix.clone(),
            request_timeout_ms: config.adapter.redis.requests_timeout,
            use_connection_manager: true,
            cluster_mode: config.adapter.redis.cluster_mode,
        };

        let connection_manager: Box<dyn Adapter + Send + Sync> =
            match RedisAdapter::new(adapter_config).await {
                Ok(adapter) => {
                    info!("Using Redis adapter");
                    Box::new(adapter)
                }
                Err(e) => {
                    warn!(
                        "Failed to initialize Redis adapter: {}, falling back to local adapter",
                        e
                    );
                    Box::new(LocalAdapter::new())
                }
            };

        // Initialize the connection manager
        let connection_manager = Arc::new(Mutex::new(connection_manager));

        // Initialize cache manager with Redis configuration
        let cache_config = RedisCacheConfig {
            url: redis_url.clone(),
            prefix: match config.cache.driver.as_str() {
                "redis" => {
                    if let Some(prefix) = config.cache.redis.redis_options.get("prefix") {
                        prefix.as_str().unwrap_or("cache").to_string()
                    } else {
                        "cache".to_string()
                    }
                }
                _ => "cache".to_string(),
            },
            ..RedisCacheConfig::default()
        };

        let cache_manager = match RedisCacheManager::new(cache_config).await {
            Ok(manager) => {
                info!("Using Redis cache manager");
                manager
            }
            Err(e) => {
                warn!("Failed to initialize Redis cache: {}", e);
                return Err(e);
            }
        };

        // Initialize channel manager
        let channel_manager =
            Arc::new(RwLock::new(ChannelManager::new(connection_manager.clone())));

        // Initialize auth validator
        let auth_validator = Arc::new(AuthValidator::new(app_manager.clone()));

        // Initialize metrics
        let metrics_driver = Arc::new(Mutex::new(
            metrics::PrometheusMetricsDriver::new(
                config.metrics.port,
                Some(&config.metrics.prometheus.prefix),
            )
            .await,
        ));

        // Initialize webhook integration
        let webhook_config = WebhookConfig {
            enabled: true,
            batching: BatchingConfig {
                enabled: config.webhooks.batching.enabled,
                duration: config.webhooks.batching.duration,
            },
            queue_driver: config.queue.driver.clone(),
            redis_url: Some(redis_url.clone()),
            redis_prefix: Some(config.adapter.redis.prefix.clone()),
            redis_concurrency: Some(config.queue.redis.concurrency as usize),
            process_id: uuid::Uuid::new_v4().to_string(),
            debug: config.debug,
        };

        // Initialize webhook integration with proper error handling
        let webhook_integration =
            match WebhookIntegration::new(webhook_config, app_manager.clone()).await {
                Ok(integration) => {
                    info!("Webhook integration initialized successfully");
                    Arc::new(integration)
                }
                Err(e) => {
                    warn!(
                        "Failed to initialize webhook integration: {}, webhooks will be disabled",
                        e
                    );
                    // Create a disabled integration as fallback
                    let disabled_config = WebhookConfig {
                        enabled: false,
                        ..Default::default()
                    };

                    Arc::new(WebhookIntegration::new(disabled_config, app_manager.clone()).await?)
                }
            };

        // Initialize the state
        let state = ServerState {
            app_manager: app_manager.clone(),
            channel_manager: channel_manager.clone(),
            connection_manager: connection_manager.clone(),
            auth_validator,
            cache_manager: Arc::new(Mutex::new(cache_manager)),
            webhooks_integration: webhook_integration.clone(),
            metrics: Some(metrics_driver.clone()),
            running: Arc::new(AtomicBool::new(true)),
        };

        // Create connection handler with webhook integration
        let handler = Arc::new(ConnectionHandler::new(
            state.app_manager.clone(),
            state.channel_manager.clone(),
            state.connection_manager.clone(),
            state.cache_manager.clone(),
            state.metrics.clone(),
            Some(webhook_integration),
        ));

        Ok(Self {
            config,
            state,
            handler,
        })
    }

    /// Initialize the server
    async fn init(&self) -> Result<()> {
        // Initialize adapter
        {
            let mut connection_manager = self.state.connection_manager.lock().await;
            connection_manager.init().await;
        }

        // Register apps from configuration
        if !self.config.app_manager.array.apps.is_empty() {
            info!(
                "Registering {} apps from configuration",
                self.config.app_manager.array.apps.len()
            );

            // Clone apps from config to avoid borrowing issues
            let apps_to_register = self.config.app_manager.array.apps.clone();

            // Register each app individually and log the outcome
            for app in apps_to_register {
                info!("Registering app: id={}, key={}", app.id, app.key);

                match self.state.app_manager.register_app(app.clone()).await {
                    Ok(_) => info!("Successfully registered app: {}", app.id),
                    Err(e) => {
                        warn!("Failed to register app {}: {}", app.id, e);
                        // Check if app already exists
                        match self.state.app_manager.get_app(&app.id).await {
                            Ok(Some(_)) => {
                                info!("App {} already exists, updating instead", app.id);
                                // Try to update instead
                                if let Err(update_err) =
                                    self.state.app_manager.update_app(app.clone()).await
                                {
                                    error!(
                                        "Failed to update existing app {}: {}",
                                        app.clone().id,
                                        update_err
                                    );
                                    // Continue with other apps rather than failing completely
                                } else {
                                    info!("Successfully updated app: {}", app.id);
                                }
                            }
                            _ => {
                                // Some other error occurred, but continue processing other apps
                                error!("Error retrieving app {}: {}", app.id, e);
                            }
                        }
                    }
                }
            }
        } else {
            // No apps in configuration, register demo app
            info!("No apps found in configuration, registering demo app");

            let demo_app = App {
                id: "demo-app".to_string(),
                key: "demo-key".to_string(),
                secret: "demo-secret".to_string(),
                enable_client_messages: true,
                enabled: true,
                max_connections: 1000,
                max_client_events_per_second: 100,
                webhooks: Some(vec![Webhook {
                    url: Some("http://localhost:3000/pusher/webhooks".parse().unwrap()),
                    lambda_function: None,
                    lambda: None,
                    event_types: vec!["member_added".to_string(), "member_removed".to_string()],
                    filter: None,
                    headers: None,
                }]),
                ..Default::default()
            };

            match self.state.app_manager.register_app(demo_app).await {
                Ok(_) => info!("Successfully registered demo app"),
                Err(e) => {
                    warn!("Failed to register demo app: {}", e);
                    // Not returning error as server can still function with no apps
                }
            }
        }

        // Verify apps were registered
        match self.state.app_manager.get_apps().await {
            Ok(apps) => {
                info!("Server has {} registered apps:", apps.len());
                for app in apps {
                    info!(
                        "- App: id={}, key={}, enabled={}",
                        app.id, app.key, app.enabled
                    );
                }
            }
            Err(e) => {
                warn!("Failed to retrieve registered apps: {}", e);
            }
        }

        info!("Server initialized successfully");
        Ok(())
    }

    /// Configure routes for the HTTP server
    fn configure_http_routes(&self) -> Router {
        // Create CORS layer
        let cors = CorsLayer::new()
            .allow_origin("*".parse::<HeaderValue>().unwrap())
            .allow_methods([Method::GET, Method::POST])
            .allow_headers([AUTHORIZATION, CONTENT_TYPE]);

        // Create application routes
        Router::new()
            // WebSocket handler for Pusher protocol
            .route("/app/{key}", get(handle_ws_upgrade))
            // HTTP API endpoints
            .route("/apps/{appId}/events", post(events))
            .route("/apps/{appId}/batch_events", post(batch_events))
            .route("/apps/{app_id}/channels", get(channels))
            .route("/apps/{app_id}/channels/{channel_name}", get(channel))
            .route(
                "/apps/{app_id}/channels/{channel_name}/users",
                get(channel_users),
            )
            .route(
                "/apps/{app_id}/users/{user_id}/terminate_connections",
                post(terminate_user_connections),
            )
            .route("/usage", get(usage))
            .route("/up/{app_id}", get(up))
            // Apply CORS middleware
            .layer(cors)
            .with_state(self.handler.clone())
    }

    /// Configure routes for the metrics server
    fn configure_metrics_routes(&self) -> Router {
        Router::new()
            .route("/metrics", get(metrics))
            .with_state(self.handler.clone())
    }

    /// Start the server
    async fn start(&self) -> Result<()> {
        info!("Starting Sockudo server...");

        // Initialize server components
        self.init().await?;

        // Configure HTTP router
        let http_router = self.configure_http_routes();

        // Configure metrics router
        let metrics_router = self.configure_metrics_routes();

        // Get addresses
        let http_addr = self.get_http_addr();
        let metrics_addr = self.get_metrics_addr();

        // Create TCP listeners
        let http_listener = TcpListener::bind(http_addr).await?;
        let metrics_listener = TcpListener::bind(metrics_addr).await?;

        info!("HTTP server listening on {}", http_addr);
        info!("Metrics server listening on {}", metrics_addr);

        // Spawn HTTP server
        let http_server = axum::serve(http_listener, http_router);
        let metrics_server = axum::serve(metrics_listener, metrics_router);

        // Get shutdown signal
        let shutdown_signal = self.shutdown_signal();

        // Spawn servers with graceful shutdown
        let running = self.state.running.clone();
        tokio::select! {
            res = http_server => {
                if let Err(err) = res {
                    error!("HTTP server error: {}", err);
                }
            }
            res = metrics_server => {
                if let Err(err) = res {
                    error!("Metrics server error: {}", err);
                }
            }
            _ = shutdown_signal => {
                info!("Shutdown signal received");
                running.store(false, Ordering::SeqCst);
            }
        }

        info!("Server shutting down");

        Ok(())
    }

    /// Graceful shutdown handler
    async fn shutdown_signal(&self) {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }

        info!("Shutdown signal received, starting graceful shutdown");
    }

    /// Stop the server
    async fn stop(&self) -> Result<()> {
        info!("Stopping server...");

        // Signal server to stop
        self.state.running.store(false, Ordering::SeqCst);

        // Wait for ongoing operations to complete
        tokio::time::sleep(Duration::from_secs(self.config.shutdown_grace_period)).await;

        // Close cache connections
        {
            let cache_manager = self.state.cache_manager.lock().await;
            let _ = cache_manager.disconnect().await;
        }

        info!("Server stopped");

        Ok(())
    }

    pub async fn load_options_from_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).await?;

        let options: ServerOptions = from_str(&contents)?;
        self.config = options;
        println!("{:?}", self.config.app_manager);

        Ok(())
    }

    /// Register multiple apps
    async fn register_apps(&self, apps: Vec<App>) -> Result<()> {
        for app in apps {
            // First check if the app already exists
            let existing_app = self.state.app_manager.get_app(&app.id).await?;

            if existing_app.is_some() {
                // App exists, update it
                self.state.app_manager.update_app(app).await?;
            } else {
                // New app, register it
                self.state.app_manager.register_app(app).await?;
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // print hello world

    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Sockudo server initialization");
    // Create default ServerOptions
    let mut config = ServerOptions::default();

    // Override from environment variables
    config.debug = std::env::var("DEBUG")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    config.host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());

    config.port = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6001);

    // Override Redis URL if provided in environment
    if let Ok(redis_url) = std::env::var("REDIS_URL") {
        // Update Redis config in adapter, making sure the HashMap exists
        if !config.adapter.redis.redis_pub_options.contains_key("url") {
            config
                .adapter
                .redis
                .redis_pub_options
                .insert("url".to_string(), json!(redis_url.clone()));
        }

        if !config.adapter.redis.redis_sub_options.contains_key("url") {
            config
                .adapter
                .redis
                .redis_sub_options
                .insert("url".to_string(), json!(redis_url.clone()));
        }

        if !config.cache.redis.redis_options.contains_key("url") {
            config
                .cache
                .redis
                .redis_options
                .insert("url".to_string(), json!(redis_url.clone()));
        }

        info!("Using Redis URL from environment: {}", redis_url);
    }

    // Load config from file if available
    let config_path =
        std::env::var("CONFIG_FILE").unwrap_or_else(|_| "src/config.json".to_string());
    if Path::new(&config_path).exists() {
        info!("Loading configuration from {}", config_path);

        // Read the file
        let mut file = match File::open(&config_path) {
            Ok(file) => file,
            Err(e) => {
                error!("Failed to open configuration file {}: {}", config_path, e);
                return Err(Error::InternalError(format!(
                    "Failed to open config file: {}",
                    e
                )));
            }
        };

        let mut contents = String::new();
        if let Err(e) = file.read_to_string(&mut contents) {
            error!("Failed to read configuration file {}: {}", config_path, e);
            return Err(Error::InternalError(format!(
                "Failed to read config file: {}",
                e
            )));
        }

        // Parse the configuration file
        match from_str::<ServerOptions>(&contents) {
            Ok(file_config) => {
                // Log apps found in config
                info!(
                    "Found {} apps in configuration file",
                    file_config.app_manager.array.apps.len()
                );

                // Log details about each app for debugging
                for app in &file_config.app_manager.array.apps {
                    info!(
                        "App in config: id={}, key={}, enabled={}",
                        app.id, app.key, app.enabled
                    );
                }

                // Replace our config with the file config
                config = file_config;

                info!("Successfully loaded configuration from {}", config_path);
            }
            Err(e) => {
                error!("Failed to parse configuration file {}: {}", config_path, e);
                return Err(Error::InternalError(format!(
                    "Failed to parse config file: {}",
                    e
                )));
            }
        }
    } else {
        info!(
            "No configuration file found at {}, using defaults and environment variables",
            config_path
        );
    }

    // Create the server with the loaded configuration
    let server = match SockudoServer::new(config).await {
        Ok(server) => server,
        Err(e) => {
            error!("Failed to create server: {}", e);
            return Err(e);
        }
    };

    let a = "Hello";
    println!("{a}");
    println!("{:?}", server.config.app_manager.array.apps);
    // Start the server and await completion
    info!("Starting Sockudo server");
    if let Err(e) = server.start().await {
        error!("Server error: {}", e);
        return Err(e);
    }

    info!("Server shutdown complete");
    Ok(())
}
