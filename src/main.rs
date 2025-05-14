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

use app::dynamodb_app_manager::DynamoDbConfig;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::uri::Authority;
use axum::http::Method;
use axum::http::{HeaderValue, StatusCode, Uri};
use axum::response::Redirect;
use axum::routing::{get, post};
use axum::{serve, BoxError, RequestExt, Router, ServiceExt};
use axum::middleware::from_fn_with_state;
use axum_extra::extract::Host;
use axum_server::tls_rustls::RustlsConfig;
use error::Error;
use serde_json::{from_str, json, Value};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{Mutex, RwLock};

use crate::adapter::local_adapter::LocalAdapter;
use crate::adapter::nats_adapter::{NatsAdapter, NatsAdapterConfig};
use crate::adapter::redis_adapter::{RedisAdapter, RedisAdapterConfig};
use crate::adapter::redis_cluster_adapter::{RedisClusterAdapter, RedisClusterAdapterConfig};
use crate::adapter::Adapter;
use crate::adapter::ConnectionHandler;
use crate::app::auth::AuthValidator;
use crate::app::config::App;
use crate::app::dynamodb_app_manager::DynamoDbAppManager;
use crate::app::manager::AppManager;
use crate::app::memory_app_manager::MemoryAppManager;
use crate::app::mysql_app_manager::MySQLAppManager;
use crate::cache::manager::CacheManager;
use crate::cache::memory_cache_manager::MemoryCacheManager;
use crate::cache::redis_cache_manager::{RedisCacheConfig, RedisCacheManager};
use crate::cache::redis_cluster_cache_manager::{
    RedisClusterCacheConfig, RedisClusterCacheManager,
};
use crate::channel::ChannelManager;
use crate::error::Result;
use crate::http_handler::{
    batch_events, channel, channel_users, channels, events, metrics, terminate_user_connections,
    up, usage,
};
use crate::log::Log;
use crate::metrics::{MetricsFactory, MetricsInterface};
use crate::options::{ServerOptions, WebhooksConfig};
use crate::queue::manager::{QueueManager, QueueManagerFactory};
use crate::rate_limiter::{create_rate_limiter, RateLimiter};
use crate::webhook::integration::{BatchingConfig, WebhookConfig, WebhookIntegration};
use crate::webhook::types::Webhook;
use crate::ws_handler::handle_ws_upgrade;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Server state containing all managers
#[derive(Clone)]
struct ServerState {
    app_manager: Arc<dyn AppManager + Send + Sync>,
    channel_manager: Arc<RwLock<ChannelManager>>,
    connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>,
    auth_validator: Arc<AuthValidator>,
    cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    queue_manager: Option<Arc<QueueManager>>,
    rate_limiter: Option<Arc<dyn RateLimiter>>,
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
        // Initialize app manager based on config
        let app_manager: Arc<dyn AppManager + Send + Sync> = match config
            .app_manager
            .driver
            .as_str()
        {
            "mysql" => {
                info!("Initializing MySQL app manager");
                let mysql_config = config.database.mysql.clone();
                match MySQLAppManager::new(mysql_config.into()).await {
                    Ok(manager) => Arc::new(manager),
                    Err(e) => {
                        warn!("Failed to initialize MySQL app manager: {}, falling back to memory manager", e);
                        Arc::new(MemoryAppManager::new())
                    }
                }
            }
            "dynamodb" => {
                info!("Initializing DynamoDB app manager");
                let dynamo_config = DynamoDbConfig {
                    region: "us-east-1".to_string(), // Default or from config
                    table_name: "sockudo-applications".to_string(), // Default or from config
                    endpoint: None,
                    access_key: None,
                    secret_key: None,
                    cache_ttl: 300,
                    profile_name: None,
                };
                match DynamoDbAppManager::new(dynamo_config).await {
                    Ok(manager) => Arc::new(manager),
                    Err(e) => {
                        warn!("Failed to initialize DynamoDB app manager: {}, falling back to memory manager", e);
                        Arc::new(MemoryAppManager::new())
                    }
                }
            }
            _ => {
                info!("Using memory app manager");
                Arc::new(MemoryAppManager::new())
            }
        };

        // Initialize adapter based on config
        let connection_manager: Box<dyn Adapter + Send + Sync> = match config
            .adapter
            .driver
            .as_str()
        {
            "redis" => {
                // Get Redis URL from adapter config or fallback
                let redis_url = if let Some(url) = config.adapter.redis.redis_pub_options.get("url")
                {
                    url.as_str().unwrap_or("redis://127.0.0.1:6379").to_string()
                } else {
                    "redis://127.0.0.1:6379".to_string()
                };

                // Initialize Redis adapter with configuration
                let adapter_config = RedisAdapterConfig {
                    url: redis_url.clone(),
                    prefix: config.adapter.redis.prefix.clone(),
                    request_timeout_ms: config.adapter.redis.requests_timeout,
                    use_connection_manager: true,
                    cluster_mode: config.adapter.redis.cluster_mode,
                };

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
                }
            }
            "redis-cluster" => {
                info!("Initializing Redis Cluster adapter");
                // Get cluster nodes from config
                let nodes = vec!["redis://127.0.0.1:7000".to_string()]; // Replace with actual config

                // Create Redis Cluster config
                let cluster_config = RedisClusterAdapterConfig {
                    nodes,
                    prefix: config.adapter.redis.prefix.clone(),
                    request_timeout_ms: config.adapter.redis.requests_timeout,
                    use_connection_manager: true,
                };

                match RedisClusterAdapter::new(cluster_config).await {
                    Ok(adapter) => {
                        info!("Using Redis Cluster adapter");
                        Box::new(adapter)
                    }
                    Err(e) => {
                        warn!("Failed to initialize Redis Cluster adapter: {}, falling back to local adapter", e);
                        Box::new(LocalAdapter::new())
                    }
                }
            }
            "nats" => {
                info!("Initializing NATS adapter");

                let nats_config = NatsAdapterConfig {
                    servers: config.adapter.nats.servers.clone(),
                    prefix: config.adapter.nats.prefix.clone(),
                    request_timeout_ms: config.adapter.nats.requests_timeout,
                    username: config.adapter.nats.user.clone(),
                    password: config.adapter.nats.pass.clone(),
                    token: config.adapter.nats.token.clone(),
                    connection_timeout_ms: config.adapter.nats.timeout,
                    nodes_number: config.adapter.nats.nodes_number,
                };

                match NatsAdapter::new(nats_config).await {
                    Ok(adapter) => {
                        info!("Using NATS adapter");
                        Box::new(adapter)
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize NATS adapter: {}, falling back to local adapter",
                            e
                        );
                        Box::new(LocalAdapter::new())
                    }
                }
            }
            _ => {
                info!("Using local adapter");
                Box::new(LocalAdapter::new())
            }
        };

        // Initialize the connection manager
        let connection_manager = Arc::new(Mutex::new(connection_manager));

        // Initialize cache manager
        let cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>> = match config
            .cache
            .driver
            .as_str()
        {
            "redis" => {
                // Get Redis URL from cache config or fallback
                let redis_url = if let Some(url) = config.cache.redis.redis_options.get("url") {
                    url.as_str().unwrap_or("redis://127.0.0.1:6379").to_string()
                } else {
                    "redis://127.0.0.1:6379".to_string()
                };

                let cache_config = RedisCacheConfig {
                    url: redis_url.clone(),
                    prefix: config
                        .cache
                        .redis
                        .redis_options
                        .get("prefix")
                        .and_then(|v| v.as_str())
                        .unwrap_or("cache")
                        .to_string(),
                    ..RedisCacheConfig::default()
                };

                match RedisCacheManager::new(cache_config).await {
                    Ok(manager) => {
                        info!("Using Redis cache manager");
                        Arc::new(Mutex::new(manager))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize Redis cache: {}, falling back to memory cache",
                            e
                        );
                        let memory_cache = MemoryCacheManager::new(Default::default());
                        Arc::new(Mutex::new(memory_cache))
                    }
                }
            }
            "redis-cluster" => {
                info!("Initializing Redis Cluster cache manager");

                // Define nodes from config
                let nodes = vec!["redis://127.0.0.1:7000".to_string()]; // Replace with actual config

                let cluster_cache_config = RedisClusterCacheConfig {
                    nodes,
                    prefix: config
                        .cache
                        .redis
                        .redis_options
                        .get("prefix")
                        .and_then(|v| v.as_str())
                        .unwrap_or("cache")
                        .to_string(),
                    ..RedisClusterCacheConfig::default()
                };

                match RedisClusterCacheManager::new(cluster_cache_config).await {
                    Ok(manager) => {
                        info!("Using Redis Cluster cache manager");
                        Arc::new(Mutex::new(manager))
                    }
                    Err(e) => {
                        warn!("Failed to initialize Redis Cluster cache: {}, falling back to memory cache", e);
                        let memory_cache = MemoryCacheManager::new(Default::default());
                        Arc::new(Mutex::new(memory_cache))
                    }
                }
            }
            _ => {
                info!("Using memory cache manager");
                let memory_cache = MemoryCacheManager::new(Default::default());
                Arc::new(Mutex::new(memory_cache))
            }
        };

        // Initialize channel manager
        let channel_manager =
            Arc::new(RwLock::new(ChannelManager::new(connection_manager.clone())));

        // Initialize auth validator
        let auth_validator = Arc::new(AuthValidator::new(app_manager.clone()));

        // Initialize metrics based on config
        let metrics = if config.metrics.enabled {
            info!(
                "Initializing metrics with driver: {}",
                config.metrics.driver
            );

            match MetricsFactory::create(
                &config.metrics.driver,
                config.metrics.port,
                Some(&config.metrics.prometheus.prefix),
            )
            .await
            {
                Some(metrics_driver) => {
                    info!("Metrics driver initialized successfully");
                    Some(metrics_driver)
                }
                None => {
                    warn!("Failed to initialize metrics driver, metrics will be disabled");
                    None
                }
            }
        } else {
            info!("Metrics are disabled in configuration");
            None
        };

        // Initialize rate limiter based on config
        let rate_limiter = match create_rate_limiter(&config.rate_limiter).await {
            Ok(limiter) => {
                info!(
                    "Rate limiter initialized with driver: {}",
                    config.rate_limiter.driver
                );
                Some(limiter)
            }
            Err(e) => {
                warn!(
                    "Failed to initialize rate limiter: {}, rate limiting will be disabled",
                    e
                );
                None
            }
        };

        // Initialize queue manager based on config
        let queue_manager = match QueueManagerFactory::create(
            &config.queue.driver,
            match config.queue.driver.as_str() {
                "redis" => {
                    if let Some(url) = config.cache.redis.redis_options.get("url") {
                        url.as_str()
                    } else {
                        Option::from("redis://127.0.0.1:6379")
                    }
                }
                _ => Option::from("redis://127.0.0.1:6379"),
            },
            Some(&config.adapter.redis.prefix),
            match config.queue.driver.as_str() {
                "redis" => Some(config.queue.redis.concurrency as usize),
                _ => Some(5),
            },
        )
        .await
        {
            Ok(queue) => {
                info!(
                    "Queue manager initialized with driver: {}",
                    config.queue.driver
                );
                Some(Arc::new(QueueManager::new(queue)))
            }
            Err(e) => {
                warn!(
                    "Failed to initialize queue manager: {}, queues will be disabled",
                    e
                );
                None
            }
        };

        // Initialize webhook integration
        let webhook_config = WebhookConfig {
            enabled: true,
            batching: BatchingConfig {
                enabled: config.webhooks.batching.enabled,
                duration: config.webhooks.batching.duration,
            },
            queue_driver: config.queue.driver.clone(),
            redis_url: if let Some(url) = config.cache.redis.redis_options.get("url") {
                Some(url.as_str().unwrap_or("redis://127.0.0.1:6379").to_string())
            } else {
                Some("redis://127.0.0.1:6379".to_string())
            },
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
            cache_manager,
            queue_manager,
            rate_limiter,
            webhooks_integration: webhook_integration.clone(),
            metrics: metrics.clone(),
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
            state.rate_limiter.clone(),
        ));

        // Initialize adapter metrics if available
        if let Some(metrics_instance) = &metrics {
            // For adapters that support metrics, set metrics based on the adapter type
            match config.adapter.driver.as_str() {
                "redis" => {
                    // If using Redis adapter, create a new one with metrics
                    if let Ok(mut redis_adapter) = RedisAdapter::new(RedisAdapterConfig {
                        url: if let Some(url) = config.adapter.redis.redis_pub_options.get("url") {
                            url.as_str().unwrap_or("redis://127.0.0.1:6379").to_string()
                        } else {
                            "redis://127.0.0.1:6379".to_string()
                        },
                        prefix: config.adapter.redis.prefix.clone(),
                        request_timeout_ms: config.adapter.redis.requests_timeout,
                        use_connection_manager: true,
                        cluster_mode: config.adapter.redis.cluster_mode,
                    })
                    .await
                    {
                        redis_adapter
                            .set_metrics(metrics_instance.clone())
                            .await
                            .ok();

                        // Replace the existing adapter with the one with metrics
                        let mut connection_manager = state.connection_manager.lock().await;
                        *connection_manager = Box::new(redis_adapter);
                    }
                }
                "redis-cluster" => {
                    // Similar approach for Redis Cluster adapter if needed
                    info!("Metrics for Redis Cluster adapter not implemented yet");
                }
                "nats" => {
                    // Similar approach for NATS adapter if needed
                    info!("Metrics for NATS adapter not implemented yet");
                }
                _ => {
                    info!("Current adapter doesn't support metrics");
                }
            }
        }

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

        // Initialize metrics if available
        if let Some(metrics) = &self.state.metrics {
            let metrics_guard = metrics.lock().await;
            if let Err(e) = metrics_guard.init().await {
                warn!("Failed to initialize metrics: {}", e);
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

        // Create rate limiter middleware based on config
        let rate_limiter_middleware = if let Some(rate_limiter) = &self.state.rate_limiter {
            // Configure rate limit options based on server config
            let rate_limit_options = rate_limiter::middleware::RateLimitOptions {
                include_headers: true,
                fail_open: false, // If rate limiter fails, block the request
                key_prefix: Some("api:".to_string()),
            };

            // Create IP-based rate limiter middleware
            // Use the configured trust_hops value or default to 1 (trust 1 proxy)
            let trust_hops = self.config.rate_limiter.api_rate_limit.trust_hops.unwrap_or(1);
            Some(rate_limiter::middleware::with_ip_limiter_trusting(
                rate_limiter.clone(),
                trust_hops as usize,
                rate_limit_options,
            ))
        } else {
            None
        };

        // Base router with all routes
        let mut router = Router::new()
            // WebSocket handler for Pusher protocol
            .route("/app/{appKey}", get(handle_ws_upgrade))
            .route("/apps/{appId}/events", post(events))
            .route("/apps/{appId}/batch_events", post(batch_events))
            .route("/apps/{appId}/channels", get(channels))
            .route("/apps/{appId}/channels/{channelName}", get(channel))
            .route(
                "/apps/{appId}/channels/{channelName}/users",
                get(channel_users),
            )
            .route(
                "/apps/{appId}/users/{userId}/terminate_connections",
                post(terminate_user_connections),
            )
            .route("/usage", get(usage))
            .route("/up/{app_id}", get(up))
            // Apply CORS middleware
            .layer(cors);

        // Apply rate limiter middleware if available
        if let Some(rate_limiter_layer) = rate_limiter_middleware {
            router = router.layer(rate_limiter_layer);
        }

        // Return the configured router
        router.with_state(self.handler.clone())
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

        // Configure metrics router if metrics are enabled
        let metrics_router = self.configure_metrics_routes();

        // Get addresses
        let http_addr = self.get_http_addr();
        let metrics_addr = self.get_metrics_addr();

        // Check if SSL is enabled and cert/key paths are provided
        if self.config.ssl.enabled
            && !self.config.ssl.cert_path.is_empty()
            && !self.config.ssl.key_path.is_empty()
        {
            info!("SSL is enabled, starting HTTPS server");

            // Load TLS configuration
            let tls_config = match self.load_tls_config().await {
                Ok(config) => config,
                Err(e) => {
                    error!("Failed to load TLS configuration: {}", e);
                    return Err(Error::InternalError(format!(
                        "Failed to load TLS configuration: {}",
                        e
                    )));
                }
            };

            // Start HTTP redirect server if enabled
            if self.config.ssl.redirect_http {
                let http_port = self.config.ssl.http_port.unwrap_or(80);
                let host_ip = self.config.host.parse::<std::net::IpAddr>()
                    .unwrap_or_else(|_| "0.0.0.0".parse().unwrap());
                let redirect_addr = SocketAddr::from((host_ip, http_port));

                info!("Starting HTTP to HTTPS redirect server on {}", redirect_addr);

                let https_port = self.config.port;

                // Create a router with the redirect handler instead of using the closure directly
                let redirect_app = Router::new().fallback(move |Host(host): Host, uri: Uri| async move {
                    match make_https(&host, uri, https_port) {
                        Ok(uri) => Ok(Redirect::permanent(&uri.to_string())),
                        Err(error) => {
                            warn!(%error, "failed to convert URI to HTTPS");
                            Err(StatusCode::BAD_REQUEST)
                        }
                    }
                });

                // Start the redirect server
                match TcpListener::bind(redirect_addr).await {
                    Ok(redirect_listener) => {
                        tokio::spawn(async move {
                            if let Err(e) = axum::serve(redirect_listener, redirect_app).await {
                                error!("HTTP redirect server error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Failed to bind HTTP redirect server: {}", e);
                    }
                }
            }

            // Start metrics server if enabled
            if self.config.metrics.enabled {
                if let Ok(metrics_listener) = TcpListener::bind(metrics_addr).await {
                    info!("Metrics server listening on {}", metrics_addr);
                    tokio::spawn(async move {
                        if let Err(e) = axum::serve(metrics_listener, metrics_router).await {
                            error!("Metrics server error: {}", e);
                        }
                    });
                } else {
                    warn!("Failed to start metrics server on {}", metrics_addr);
                }
            }

            // In the SockudoServer::start() method

            // Start main HTTPS server
            info!("HTTPS server listening on {}", http_addr);
            let running = self.state.running.clone();

            // Bind with TLS config - this returns a Server directly, not a Result
            let server = axum_server::bind_rustls(http_addr, tls_config);

            // Serve with graceful shutdown
            tokio::select! {
                result = server.serve(http_router.into_make_service()) => {
                    if let Err(err) = result {
                        error!("HTTPS server error: {}", err);
                    }
                }
                _ = self.shutdown_signal() => {
                    info!("Shutdown signal received");
                    running.store(false, Ordering::SeqCst);
                }
            }
        } else {
            // SSL is not enabled, start HTTP server as before
            info!("SSL is not enabled, starting HTTP server");

            // Create TCP listeners
            let http_listener = TcpListener::bind(http_addr).await?;
            let metrics_listener = if self.config.metrics.enabled {
                match TcpListener::bind(metrics_addr).await {
                    Ok(listener) => {
                        info!("Metrics server listening on {}", metrics_addr);
                        Some(listener)
                    }
                    Err(e) => {
                        warn!("Failed to bind metrics server: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            info!("HTTP server listening on {}", http_addr);

            // Spawn servers with graceful shutdown
            let running = self.state.running.clone();

            if let Some(metrics_listener) = metrics_listener {
                let metrics_router_clone = metrics_router.clone();
                tokio::spawn(async move {
                    if let Err(e) = axum::serve(metrics_listener, metrics_router_clone).await {
                        error!("Metrics server error: {}", e);
                    }
                });
            }

            // Start HTTP server with shutdown signal
            let http_server = axum::serve(http_listener, http_router);

            tokio::select! {
                res = http_server => {
                    if let Err(err) = res {
                        error!("HTTP server error: {}", err);
                    }
                }
                _ = self.shutdown_signal() => {
                    info!("Shutdown signal received");
                    running.store(false, Ordering::SeqCst);
                }
            }
        }

        info!("Server shutting down");
        Ok(())
    }

    /// Load TLS configuration from the SSL config
    async fn load_tls_config(&self) -> Result<RustlsConfig> {
        // Handle certificate and key paths
        let cert_path = std::path::PathBuf::from(&self.config.ssl.cert_path);
        let key_path = std::path::PathBuf::from(&self.config.ssl.key_path);

        // If passphrase is provided, we need to use a different loading method
        // Currently, the example only supports loading from PEM files without passphrase
        // For passphrase support, we'd need to implement more complex loading logic

        // Load TLS configuration
        RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| Error::InternalError(format!("Failed to load TLS configuration: {}", e)))
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

        // Close queue connections if available
        if let Some(queue_manager) = &self.state.queue_manager {
            let _ = queue_manager.disconnect().await;
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

    config.host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

    config.port = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6001);

    // Override adapter options if provided in environment
    if let Ok(adapter_driver) = std::env::var("ADAPTER_DRIVER") {
        config.adapter.driver = adapter_driver;
    }

    if let Ok(ssl_enabled) = std::env::var("SSL_ENABLED") {
        config.ssl.enabled = ssl_enabled == "1" || ssl_enabled.to_lowercase() == "true";
    }

    if let Ok(cert_path) = std::env::var("SSL_CERT_PATH") {
        config.ssl.cert_path = cert_path;
    }

    if let Ok(key_path) = std::env::var("SSL_KEY_PATH") {
        config.ssl.key_path = key_path;
    }

    if let Ok(redirect_http) = std::env::var("SSL_REDIRECT_HTTP") {
        config.ssl.redirect_http = redirect_http == "1" || redirect_http.to_lowercase() == "true";
    }

    if let Ok(http_port) = std::env::var("SSL_HTTP_PORT") {
        if let Ok(port) = http_port.parse() {
            config.ssl.http_port = Some(port);
        }
    }

    // Override Redis URL if provided in environment
    if let Ok(redis_url) = std::env::var("REDIS_URL") {
        // Update Redis config in adapter, making sure the HashMap exists
        config
            .adapter
            .redis
            .redis_pub_options
            .insert("url".to_string(), json!(redis_url.clone()));
        config
            .adapter
            .redis
            .redis_sub_options
            .insert("url".to_string(), json!(redis_url.clone()));
        config
            .cache
            .redis
            .redis_options
            .insert("url".to_string(), json!(redis_url.clone()));

        // For queue manager
        // Redis queue options update if needed

        info!("Using Redis URL from environment: {}", redis_url);
    }

    // Override NATS options if provided in environment
    if let Ok(nats_url) = std::env::var("NATS_URL") {
        config.adapter.nats.servers = vec![nats_url.clone()];
        info!("Using NATS URL from environment: {}", nats_url);
    }

    // Override cache driver if provided in environment
    if let Ok(cache_driver) = std::env::var("CACHE_DRIVER") {
        config.cache.driver = cache_driver;
    }

    // Override queue driver if provided in environment
    if let Ok(queue_driver) = std::env::var("QUEUE_DRIVER") {
        config.queue.driver = queue_driver;
    }

    // Override metrics options if provided in environment
    if let Ok(metrics_enabled) = std::env::var("METRICS_ENABLED") {
        config.metrics.enabled = metrics_enabled == "1" || metrics_enabled.to_lowercase() == "true";
    }

    if let Ok(metrics_driver) = std::env::var("METRICS_DRIVER") {
        config.metrics.driver = metrics_driver;
    }

    if let Ok(metrics_port) = std::env::var("METRICS_PORT") {
        if let Ok(port) = metrics_port.parse() {
            config.metrics.port = port;
        }
    }

    // Override rate limiter options if provided in environment
    if let Ok(rate_limiter_driver) = std::env::var("RATE_LIMITER_DRIVER") {
        config.rate_limiter.driver = rate_limiter_driver;
    }

    // Load config from file if available
    let config_path = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "src/config.json".to_string());
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

    // Start the server and await completion
    info!("Starting Sockudo server");
    if let Err(e) = server.start().await {
        error!("Server error: {}", e);
        return Err(e);
    }

    info!("Server shutdown complete");
    Ok(())
}

fn make_https(host: &str, uri: Uri, https_port: u16) -> core::result::Result<Uri, BoxError> {
    let mut parts = uri.into_parts();
    parts.scheme = Some(axum::http::uri::Scheme::HTTPS);
    if parts.path_and_query.is_none() {
        parts.path_and_query = Some("/".parse().unwrap());
    }
    let authority: Authority = host.parse()?;
    let bare_host = match authority.port() {
        Some(port_struct) => authority
            .as_str()
            .strip_suffix(&format!(":{}", port_struct))
            .unwrap_or(authority.as_str()),
        None => authority.as_str(),
    };
    parts.authority = Some(format!("{bare_host}:{https_port}").parse()?);
    Ok(Uri::from_parts(parts)?)
}