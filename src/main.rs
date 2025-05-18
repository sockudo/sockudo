mod adapter;
mod app;
mod cache;
mod channel;
mod error;
mod http_handler;
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

use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::http::header::HeaderName;
use axum::http::uri::Authority;
use axum::http::Method;
use axum::http::{HeaderValue, StatusCode, Uri};
use axum::response::Redirect;
use axum::routing::{get, post};
use axum::{BoxError, Router, ServiceExt};

use axum_extra::extract::Host;
use axum_server::tls_rustls::RustlsConfig;
use error::Error;
use serde_json::{from_str, json};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{Mutex, RwLock};

// Updated factory imports
use crate::adapter::factory::AdapterFactory;
use crate::app::factory::AppManagerFactory;
use crate::cache::factory::CacheManagerFactory;
use crate::channel::ChannelManager;
use crate::error::Result;
use crate::http_handler::{
    batch_events, channel, channel_users, channels, events, metrics, terminate_user_connections,
    up, usage,
};

use crate::metrics::MetricsFactory;
use crate::options::{AdapterDriver, QueueDriver, ServerOptions};
use crate::queue::manager::{QueueManager, QueueManagerFactory};
use crate::rate_limiter::factory::RateLimiterFactory;
use crate::rate_limiter::middleware::IpKeyExtractor;
use crate::rate_limiter::RateLimiter;
use crate::webhook::integration::{BatchingConfig, WebhookConfig, WebhookIntegration};
use crate::ws_handler::handle_ws_upgrade;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Import concrete adapter types for downcasting if set_metrics is specific
use crate::adapter::local_adapter::LocalAdapter;
use crate::adapter::nats_adapter::NatsAdapter;
use crate::adapter::redis_adapter::RedisAdapter;
use crate::adapter::redis_cluster_adapter::RedisClusterAdapter;
use crate::adapter::Adapter;
use crate::adapter::ConnectionHandler;
use crate::app::auth::AuthValidator;
use crate::app::config::App;
// AppManager trait and concrete types
use crate::app::manager::AppManager;
// CacheManager trait and concrete types
use crate::cache::manager::CacheManager;
use crate::cache::memory_cache_manager::MemoryCacheManager; // Import for fallback
                                                            // MetricsInterface trait
use crate::metrics::MetricsInterface;
use crate::websocket::WebSocketRef;

/// Server state containing all managers
#[derive(Clone)]
struct ServerState {
    app_manager: Arc<dyn AppManager + Send + Sync>,
    channel_manager: Arc<RwLock<ChannelManager>>,
    connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>,
    auth_validator: Arc<AuthValidator>,
    cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    queue_manager: Option<Arc<QueueManager>>,
    webhooks_integration: Arc<WebhookIntegration>,
    metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
    running: Arc<AtomicBool>,
    http_api_rate_limiter: Option<Arc<dyn RateLimiter + Send + Sync>>,
    debug_enabled: bool,
}

/// Main server struct
struct SockudoServer {
    config: ServerOptions,
    state: ServerState,
    handler: Arc<ConnectionHandler>,
}

impl SockudoServer {
    fn get_http_addr(&self) -> SocketAddr {
        format!("{}:{}", self.config.host, self.config.port)
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:6001".parse().unwrap())
    }

    fn get_metrics_addr(&self) -> SocketAddr {
        format!("{}:{}", self.config.metrics.host, self.config.metrics.port)
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:9601".parse().unwrap())
    }

    async fn new(config: ServerOptions) -> Result<Self> {
        let debug_enabled = config.debug;
        info!(
            "{}",
            "Initializing Sockudo server with new configuration...".to_string()
        );

        let app_manager = AppManagerFactory::create(&config.app_manager, &config.database).await?;
        info!(
            "{}",
            format!(
                "AppManager initialized with driver: {:?}",
                config.app_manager.driver
            )
        );

        let connection_manager_box =
            AdapterFactory::create(&config.adapter, &config.database, debug_enabled).await?;
        let connection_manager_arc = Arc::new(Mutex::new(connection_manager_box));
        info!(
            "{}",
            format!(
                "Adapter initialized with driver: {:?}",
                config.adapter.driver
            )
        );

        let cache_manager =
            CacheManagerFactory::create(&config.cache, &config.database.redis, debug_enabled)
                .await
                .unwrap_or_else(|e| {
                    warn!(
                        "{}",
                        format!(
                            "CacheManagerFactory creation failed: {}. Using a NoOp (Memory) Cache.",
                            e
                        )
                    );
                    let fallback_cache_options = config.cache.memory.clone();
                    // Corrected: Remove Box::new() to match expected Arc<Mutex<dyn CacheManager...>>
                    Arc::new(Mutex::new(MemoryCacheManager::new(
                        "fallback_cache".to_string(),
                        fallback_cache_options,
                    )))
                });
        info!(
            "{}",
            format!(
                "CacheManager initialized with driver: {:?}",
                config.cache.driver
            )
        );

        let channel_manager = Arc::new(RwLock::new(ChannelManager::new(
            connection_manager_arc.clone(),
        )));
        let auth_validator = Arc::new(AuthValidator::new(app_manager.clone()));

        let metrics = if config.metrics.enabled {
            info!(
                "{}",
                format!(
                    "Initializing metrics with driver: {:?}",
                    config.metrics.driver
                )
            );
            match MetricsFactory::create(
                config.metrics.driver.as_ref(), // Corrected: Convert enum to &str
                config.metrics.port,
                Some(&config.metrics.prometheus.prefix),
            )
            .await
            {
                Some(metrics_driver) => {
                    info!("{}", "Metrics driver initialized successfully".to_string());
                    Some(metrics_driver)
                }
                None => {
                    warn!(
                        "{}",
                        "Failed to initialize metrics driver, metrics will be disabled".to_string(),
                    );
                    None
                }
            }
        } else {
            info!("{}", "Metrics are disabled in configuration".to_string());
            None
        };

        let http_api_rate_limiter_instance = if config.rate_limiter.enabled {
            RateLimiterFactory::create(
                &config.rate_limiter,
                &config.database.redis,
                debug_enabled
            ).await.unwrap_or_else(|e| {
                error!("{}", format!("Failed to initialize HTTP API rate limiter: {}. Using a permissive limiter.", e));
                Arc::new(rate_limiter::memory_limiter::MemoryRateLimiter::new(u32::MAX, 1))
            })
        } else {
            info!(
                "{}",
                "HTTP API Rate limiting is globally disabled. Using a permissive limiter."
                    .to_string(),
            );
            Arc::new(rate_limiter::memory_limiter::MemoryRateLimiter::new(
                u32::MAX,
                1,
            ))
        };
        info!(
            "{}",
            format!(
                "HTTP API RateLimiter initialized (enabled: {}) with driver: {:?}",
                config.rate_limiter.enabled, config.rate_limiter.driver
            )
        );

        let owned_default_queue_redis_url: String; // Will hold the String if default is used
        let queue_redis_url_arg: Option<&str>;

        if let Some(url_override) = config.queue.redis.url_override.as_ref() {
            queue_redis_url_arg = Some(url_override.as_str());
        } else {
            owned_default_queue_redis_url = format!(
                "redis://{}:{}",
                config.database.redis.host, config.database.redis.port
            );
            queue_redis_url_arg = Some(&owned_default_queue_redis_url);
        }

        let queue_manager_opt = if config.queue.driver != QueueDriver::None {
            match QueueManagerFactory::create(
                config.queue.driver.as_ref(),
                queue_redis_url_arg, // Use the correctly scoped reference
                Some(
                    config
                        .queue
                        .redis
                        .prefix
                        .as_deref()
                        .unwrap_or("sockudo_queue:"),
                ),
                Some(config.queue.redis.concurrency as usize),
            )
            .await
            {
                Ok(queue_driver_impl) => {
                    info!(
                        "{}",
                        format!(
                            "Queue manager initialized with driver: {:?}",
                            config.queue.driver
                        )
                    );
                    Some(Arc::new(QueueManager::new(queue_driver_impl)))
                }
                Err(e) => {
                    warn!("{}", format!("Failed to initialize queue manager with driver '{:?}': {}, queues will be disabled", config.queue.driver, e));
                    None
                }
            }
        } else {
            info!(
                "{}",
                "Queue driver set to None, queue manager will be disabled.".to_string()
            );
            None
        };

        let webhook_redis_url = format!(
            "redis://{}:{}",
            config.database.redis.host, config.database.redis.port
        );

        let webhook_config_for_integration = WebhookConfig {
            enabled: true, // Consider if this should be configurable via config.webhooks.enabled
            batching: BatchingConfig {
                enabled: config.webhooks.batching.enabled,
                duration: config.webhooks.batching.duration,
            },
            queue_driver: config.queue.driver.as_ref().to_string(),
            redis_url: Some(webhook_redis_url), // ⬅️ USE THE CORRECTLY FORMATTED URL
            redis_prefix: Some(config.database.redis.key_prefix.clone() + "webhooks:"),
            redis_concurrency: Some(config.queue.redis.concurrency as usize),
            process_id: config.instance.process_id.clone(),
            debug: config.debug,
        };

        let webhook_integration = match WebhookIntegration::new(
            webhook_config_for_integration,
            app_manager.clone(),
        )
        .await
        {
            Ok(integration) => {
                info!(
                    "{}",
                    "Webhook integration initialized successfully".to_string()
                );
                Arc::new(integration)
            }
            Err(e) => {
                warn!(
                    "{}",
                    format!(
                        "Failed to initialize webhook integration: {}, webhooks will be disabled",
                        e
                    )
                );
                let disabled_config = WebhookConfig {
                    enabled: false,
                    ..Default::default()
                };
                Arc::new(WebhookIntegration::new(disabled_config, app_manager.clone()).await?)
            }
        };

        let state = ServerState {
            app_manager: app_manager.clone(),
            channel_manager: channel_manager.clone(),
            connection_manager: connection_manager_arc.clone(),
            auth_validator,
            cache_manager,
            queue_manager: queue_manager_opt,
            webhooks_integration: webhook_integration.clone(),
            metrics: metrics.clone(),
            running: Arc::new(AtomicBool::new(true)),
            http_api_rate_limiter: Some(http_api_rate_limiter_instance.clone()),
            debug_enabled,
        };

        let handler = Arc::new(ConnectionHandler::new(
            state.app_manager.clone(),
            state.channel_manager.clone(),
            state.connection_manager.clone(),
            state.cache_manager.clone(),
            state.metrics.clone(),
            Some(webhook_integration),
            state.http_api_rate_limiter.clone(),
        ));

        if let Some(metrics_instance_arc) = &metrics {
            let mut connection_manager_guard = state.connection_manager.lock().await;
            let adapter_as_any: &mut dyn std::any::Any = connection_manager_guard.as_any_mut();

            match config.adapter.driver {
                AdapterDriver::Redis => {
                    if let Some(adapter_mut) = adapter_as_any.downcast_mut::<RedisAdapter>() {
                        adapter_mut
                            .set_metrics(metrics_instance_arc.clone())
                            .await
                            .ok();
                        info!("{}", "Set metrics for RedisAdapter".to_string());
                    } else {
                        warn!(
                            "{}",
                            "Failed to downcast to RedisAdapter for metrics setup".to_string(),
                        );
                    }
                }
                AdapterDriver::Nats => {
                    if let Some(adapter_mut) = adapter_as_any.downcast_mut::<NatsAdapter>() {
                        adapter_mut
                            .set_metrics(metrics_instance_arc.clone())
                            .await
                            .ok();
                        info!("{}", "Set metrics for NatsAdapter".to_string());
                    } else {
                        warn!(
                            "{}",
                            "Failed to downcast to NatsAdapter for metrics setup".to_string(),
                        );
                    }
                }
                AdapterDriver::RedisCluster => {
                    if let Some(_adapter_mut) = adapter_as_any.downcast_mut::<RedisClusterAdapter>()
                    {
                        // Assuming RedisClusterAdapter has a similar set_metrics method
                        // If not, this needs to be implemented or handled differently
                        info!(
                            "{}",
                            "Metrics setup for RedisClusterAdapter (set_metrics call placeholder)"
                                .to_string(),
                        );
                    } else {
                        warn!(
                            "{}",
                            "Failed to downcast to RedisClusterAdapter for metrics setup"
                                .to_string(),
                        );
                    }
                }
                AdapterDriver::Local => {
                    if let Some(_adapter_mut) = adapter_as_any.downcast_mut::<LocalAdapter>() {
                        // LocalAdapter might not need/have a set_metrics method if metrics are handled globally
                        info!(
                            "{}",
                            "Metrics setup for LocalAdapter (if applicable)".to_string()
                        );
                    } else {
                        warn!(
                            "{}",
                            "Failed to downcast to LocalAdapter for metrics setup".to_string(),
                        );
                    }
                }
            }
        }
        Ok(Self {
            config,
            state,
            handler,
        })
    }

    async fn init(&self) -> Result<()> {
        info!("{}", "Server init sequence started.".to_string());
        {
            let mut connection_manager = self.state.connection_manager.lock().await;
            connection_manager.init().await;
        }

        if !self.config.app_manager.array.apps.is_empty() {
            info!(
                "{}",
                format!(
                    "Registering {} apps from configuration",
                    self.config.app_manager.array.apps.len()
                )
            );
            let apps_to_register = self.config.app_manager.array.apps.clone();
            for app in apps_to_register {
                info!(
                    "{}",
                    format!("Registering app: id={}, key={}", app.id, app.key)
                );
                match self.state.app_manager.create_app(app.clone()).await {
                    Ok(_) => info!("{}", format!("Successfully registered app: {}", app.id)),
                    Err(e) => {
                        warn!("{}", format!("Failed to register app {}: {}", app.id, e));
                        match self.state.app_manager.find_by_id(&app.id).await {
                            Ok(Some(_)) => {
                                info!(
                                    "{}",
                                    format!("App {} already exists, updating instead", app.id)
                                );
                                if let Err(update_err) =
                                    self.state.app_manager.update_app(app.clone()).await
                                {
                                    error!(
                                        "{}",
                                        format!(
                                            "Failed to update existing app {}: {}",
                                            app.id, update_err
                                        )
                                    );
                                } else {
                                    info!("{}", format!("Successfully updated app: {}", app.id));
                                }
                            }
                            _ => error!("{}", format!("Error retrieving app {}: {}", app.id, e)),
                        }
                    }
                }
            }
        } else {
            info!(
                "{}",
                "No apps found in configuration, registering demo app".to_string()
            );
            let demo_app = App {
                id: "demo-app".to_string(),
                key: "demo-key".to_string(),
                secret: "demo-secret".to_string(),
                enable_client_messages: true,
                enabled: true,
                max_connections: 1000,
                max_client_events_per_second: 100,
                webhooks: Some(vec![crate::webhook::types::Webhook {
                    url: Some("http://localhost:3000/pusher/webhooks".parse().unwrap()),
                    lambda_function: None,
                    lambda: None,
                    event_types: vec!["member_added".to_string(), "member_removed".to_string()],
                    filter: None,
                    headers: None,
                }]),
                ..Default::default()
            };
            match self.state.app_manager.create_app(demo_app).await {
                // demo_app is moved here
                Ok(_) => info!("{}", "Successfully registered demo app".to_string()),
                Err(e) => warn!("{}", format!("Failed to register demo app: {}", e)),
            }
        }

        match self.state.app_manager.get_apps().await {
            Ok(apps) => {
                info!("{}", format!("Server has {} registered apps:", apps.len()));
                for app in apps {
                    info!(
                        "{}",
                        format!(
                            "- App: id={}, key={}, enabled={}",
                            app.id, app.key, app.enabled
                        )
                    );
                }
            }
            Err(e) => warn!("{}", format!("Failed to retrieve registered apps: {}", e)),
        }

        if let Some(metrics) = &self.state.metrics {
            let metrics_guard = metrics.lock().await;
            if let Err(e) = metrics_guard.init().await {
                warn!("{}", format!("Failed to initialize metrics: {}", e));
            }
        }
        info!("{}", "Server init sequence completed.".to_string());
        Ok(())
    }

    fn configure_http_routes(&self) -> Router {
        let mut cors_builder = CorsLayer::new()
            .allow_methods(
                self.config
                    .cors
                    .methods
                    .iter()
                    .map(|s| Method::from_str(s).expect("Failed to parse CORS method"))
                    .collect::<Vec<_>>(),
            )
            .allow_headers(
                self.config
                    .cors
                    .allowed_headers
                    .iter()
                    .map(|s| HeaderName::from_str(s).expect("Failed to parse CORS header"))
                    .collect::<Vec<_>>(),
            );

        // Determine if we are using AllowOrigin::any()
        let use_allow_origin_any = self.config.cors.origin.contains(&"*".to_string())
            || self.config.cors.origin.contains(&"Any".to_string())
            || self.config.cors.origin.contains(&"any".to_string());

        if use_allow_origin_any {
            cors_builder = cors_builder.allow_origin(AllowOrigin::any());
            // If origin is '*', credentials cannot be true.
            // Override config if it's misconfigured for this scenario.
            if self.config.cors.credentials {
                warn!("{}",
                    "CORS config: 'Access-Control-Allow-Credentials' was true but 'Access-Control-Allow-Origin' is '*'. \
                    Forcing credentials to false to comply with CORS specification."
                        .to_string());
                cors_builder = cors_builder.allow_credentials(false);
            }
            if self.config.cors.origin.len() > 1 {
                warn!("{}",  "CORS config: Wildcard '*' or 'Any' is present in origins list along with other specific origins. Wildcard will take precedence, allowing all origins.".to_string());
            }
        } else if !self.config.cors.origin.is_empty() {
            let origins = self
                .config
                .cors
                .origin
                .iter()
                .map(|s| {
                    s.parse::<HeaderValue>()
                        .expect("Failed to parse CORS origin")
                })
                .collect::<Vec<_>>();
            cors_builder = cors_builder.allow_origin(AllowOrigin::list(origins));
            // Only allow credentials if specific origins are listed and config allows it
            cors_builder = cors_builder.allow_credentials(self.config.cors.credentials);
        } else {
            warn!("{}", "CORS origins list is empty. Behavior will depend on tower-http defaults (likely restrictive). Consider setting origins or '*' for AllowOrigin::any().".to_string());
            // When no origins are specified, also ensure credentials are not misconfigured if you want to be explicit
            // For example, if the default behavior of tower-http when .allow_origin is not called is to disallow,
            // then allow_credentials(true) would also be problematic.
            // If relying on tower-http default, and credentials config is true, it might also lead to issues.
            // For safety when origins is empty, you might set credentials to false too, or ensure tower-http's default
            // for allow_origin is compatible with your credentials setting.
            // Let's assume for now that if origins is empty, we also shouldn't send credentials.
            if self.config.cors.credentials {
                warn!("{}", "CORS origins list is empty, and credentials set to true. Forcing credentials to false for safety.".to_string());
                cors_builder = cors_builder.allow_credentials(false);
            }
        }

        let cors = cors_builder;

        let rate_limiter_middleware_layer = if self.config.rate_limiter.enabled {
            if let Some(rate_limiter_instance) = &self.state.http_api_rate_limiter {
                let options = crate::rate_limiter::middleware::RateLimitOptions {
                    include_headers: true,
                    fail_open: false,
                    key_prefix: Some("api:".to_string()),
                };
                let trust_hops = self
                    .config
                    .rate_limiter
                    .api_rate_limit
                    .trust_hops
                    .unwrap_or(0) as usize;
                let ip_key_extractor = IpKeyExtractor::new(trust_hops);

                info!(
                    "{}",
                    format!(
                        "Applying custom rate limiting middleware with trust_hops: {}",
                        trust_hops
                    )
                );
                Some(
                    crate::rate_limiter::middleware::RateLimitLayer::with_options(
                        rate_limiter_instance.clone(),
                        ip_key_extractor,
                        options,
                    ),
                )
            } else {
                warn!("{}", "Rate limiting is enabled in config, but no RateLimiter instance found in server state for HTTP API.".to_string());
                None
            }
        } else {
            info!(
                "{}",
                "Custom HTTP API Rate limiting is disabled in configuration.".to_string()
            );
            None
        };

        let mut router = Router::new()
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
            .route("/up/{appId}", get(up))
            .layer(cors); // Apply the configured CORS layer

        if let Some(middleware) = rate_limiter_middleware_layer {
            router = router.layer(middleware);
        }

        router.with_state(self.handler.clone())
    }

    fn configure_metrics_routes(&self) -> Router {
        Router::new()
            .route("/metrics", get(metrics))
            .with_state(self.handler.clone())
    }

    async fn start(&self) -> Result<()> {
        info!("{}", "Starting Sockudo server services...".to_string());
        self.init().await?;

        let http_router = self.configure_http_routes();
        let metrics_router = self.configure_metrics_routes();

        let http_addr = self.get_http_addr();
        let metrics_addr = self.get_metrics_addr();

        if self.config.ssl.enabled
            && !self.config.ssl.cert_path.is_empty()
            && !self.config.ssl.key_path.is_empty()
        {
            info!("{}", "SSL is enabled, starting HTTPS server".to_string());
            let tls_config = self.load_tls_config().await?;

            if self.config.ssl.redirect_http {
                let http_port = self.config.ssl.http_port.unwrap_or(80);
                let host_ip = self
                    .config
                    .host
                    .parse::<std::net::IpAddr>()
                    .unwrap_or_else(|_| "0.0.0.0".parse().unwrap());
                let redirect_addr = SocketAddr::from((host_ip, http_port));
                info!(
                    "{}",
                    format!(
                        "Starting HTTP to HTTPS redirect server on {}",
                        redirect_addr
                    )
                );
                let https_port = self.config.port;
                let redirect_app =
                    Router::new().fallback(move |Host(host): Host, uri: Uri| async move {
                        match make_https(&host, uri, https_port) {
                            Ok(uri_https) => Ok(Redirect::permanent(&uri_https.to_string())),
                            Err(error) => {
                                error!(error, "failed to convert URI to HTTPS");
                                Err(StatusCode::BAD_REQUEST)
                            }
                        }
                    });
                match TcpListener::bind(redirect_addr).await {
                    Ok(redirect_listener) => {
                        tokio::spawn(async move {
                            if let Err(e) = axum::serve(
                                redirect_listener,
                                redirect_app.into_make_service_with_connect_info::<SocketAddr>(),
                            )
                            .await
                            {
                                error!("HTTP redirect server error: {}", e);
                            }
                        });
                    }
                    Err(e) => warn!("{}", format!("Failed to bind HTTP redirect server: {}", e)),
                }
            }

            if self.config.metrics.enabled {
                if let Ok(metrics_listener) = TcpListener::bind(metrics_addr).await {
                    info!(
                        "{}",
                        format!("Metrics server listening on {}", metrics_addr)
                    );
                    let metrics_router_clone = metrics_router.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            axum::serve(metrics_listener, metrics_router_clone.into_make_service())
                                .await
                        {
                            error!("Metrics server error: {}", e);
                        }
                    });
                } else {
                    warn!(
                        "{}",
                        format!("Failed to start metrics server on {}", metrics_addr)
                    );
                }
            }

            info!("{}", format!("HTTPS server listening on {}", http_addr));
            let running = self.state.running.clone();
            let server = axum_server::bind_rustls(http_addr, tls_config);
            tokio::select! {
                result = server.serve(http_router.into_make_service_with_connect_info::<SocketAddr>()) => {
                    if let Err(err) = result { error!("HTTPS server error: {}", err); }
                }
                _ = self.shutdown_signal() => {
                    info!("{}", "Shutdown signal received for HTTPS server".to_string());
                    running.store(false, Ordering::SeqCst);
                }
            }
        } else {
            info!("{}", "SSL is not enabled, starting HTTP server".to_string());
            let http_listener = TcpListener::bind(http_addr).await?;
            let metrics_listener_opt = if self.config.metrics.enabled {
                match TcpListener::bind(metrics_addr).await {
                    Ok(listener) => {
                        info!(
                            "{}",
                            format!("Metrics server listening on {}", metrics_addr)
                        );
                        Some(listener)
                    }
                    Err(e) => {
                        warn!("{}", format!("Failed to bind metrics server: {}", e));
                        None
                    }
                }
            } else {
                None
            };

            info!("{}", format!("HTTP server listening on {}", http_addr));
            let running = self.state.running.clone();

            if let Some(metrics_listener) = metrics_listener_opt {
                let metrics_router_clone = metrics_router.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        axum::serve(metrics_listener, metrics_router_clone.into_make_service())
                            .await
                    {
                        error!("Metrics server error: {}", e);
                    }
                });
            }

            let http_server = axum::serve(
                http_listener,
                http_router.into_make_service_with_connect_info::<SocketAddr>(),
            );
            tokio::select! {
                res = http_server => {
                    if let Err(err) = res { error!("HTTP server error: {}", err); }
                }
                _ = self.shutdown_signal() => {
                    info!("{}", "Shutdown signal received for HTTP server".to_string());
                    running.store(false, Ordering::SeqCst);
                }
            }
        }
        info!("{}", "Server shutting down".to_string());
        Ok(())
    }

    async fn load_tls_config(&self) -> Result<RustlsConfig> {
        let cert_path = std::path::PathBuf::from(&self.config.ssl.cert_path);
        let key_path = std::path::PathBuf::from(&self.config.ssl.key_path);
        RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| Error::InternalError(format!("Failed to load TLS configuration: {}", e)))
    }

    async fn shutdown_signal(&self) {
        // cleanup the connections and adapters

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
            _ = ctrl_c => {
                self.stop().await.expect("Failed to stop server");
            },
            _ = terminate => {
                self.stop().await.expect("Failed to stop server");
            },
        }
        info!(
            "{}",
            "Shutdown signal received, starting graceful shutdown".to_string()
        );
    }

    #[allow(dead_code)]
    async fn stop(&self) -> Result<()> {
        let mut connection_manager = self.state.connection_manager.lock().await;
        let namespaces = connection_manager.get_namespaces().await;
        match namespaces {
            Ok(namespaces) => {
                for (app_id, namespace) in namespaces {
                    let sockets= namespace.get_sockets().await?;
                    for (_socket_id, ws) in sockets {
                        connection_manager.cleanup_connection(app_id.as_str(), WebSocketRef(ws)).await;
                    }
                }
            }
            Err(e) => warn!("{}", format!("Failed to get namespaces: {}", e)),
        }
        info!("{}", "Stopping server...".to_string());

        self.state.running.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(self.config.shutdown_grace_period)).await;
        {
            let mut cache_manager_locked = self.state.cache_manager.lock().await;
            cache_manager_locked.disconnect().await?;
        }
        if let Some(queue_manager_arc) = &self.state.queue_manager {
            queue_manager_arc.disconnect().await?;
        }
        info!("{}", "Server stopped".to_string());
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn load_options_from_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).await?;
        let options: ServerOptions = from_str(&contents)?;
        self.config = options;
        info!(
            "{}",
            format!(
                "Loaded options from file, app_manager config: {:?}",
                self.config.app_manager
            )
        );
        Ok(())
    }

    #[allow(dead_code)]
    async fn register_apps(&self, apps: Vec<App>) -> Result<()> {
        let debug_enabled = self.config.debug;
        for app in apps {
            let existing_app = self.state.app_manager.find_by_id(&app.id).await?;
            if existing_app.is_some() {
                info!("{}", format!("Updating app: {}", app.id));
                self.state.app_manager.update_app(app).await?;
            } else {
                info!("{}", format!("Registering new app: {}", app.id));
                self.state.app_manager.create_app(app).await?;
            }
        }
        Ok(())
    }
}

// Helper function to parse string to enum
fn parse_driver_enum<T: FromStr + Default + std::fmt::Debug>(
    driver_str: String,
    default_driver: T,
    driver_name: &str,
) -> T
where
    <T as FromStr>::Err: std::fmt::Debug,
{
    match T::from_str(&driver_str.to_lowercase()) {
        Ok(driver_enum) => driver_enum,
        Err(e) => {
            warn!(
                "{}",
                format!(
                    "Failed to parse {} driver '{}': {:?}. Using default: {:?}.",
                    driver_name, driver_str, e, default_driver
                )
            );
            default_driver
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sockudo=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let initial_debug = std::env::var("DEBUG")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    info!(
        "{}",
        "Starting Sockudo server initialization process...".to_string()
    );

    let mut config = ServerOptions::default();

    config.debug = initial_debug;
    if let Ok(host) = std::env::var("HOST") {
        config.host = host;
    }
    if let Ok(port_str) = std::env::var("PORT") {
        if let Ok(port) = port_str.parse() {
            config.port = port;
        } else {
            warn!(
                "{}",
                format!("Failed to parse PORT env var: '{}'", port_str)
            );
        }
    }

    if let Ok(driver_str) = std::env::var("ADAPTER_DRIVER") {
        config.adapter.driver = parse_driver_enum(driver_str, config.adapter.driver, "Adapter");
    }
    if let Ok(driver_str) = std::env::var("CACHE_DRIVER") {
        config.cache.driver = parse_driver_enum(driver_str, config.cache.driver, "Cache");
    }
    if let Ok(driver_str) = std::env::var("QUEUE_DRIVER") {
        config.queue.driver = parse_driver_enum(driver_str, config.queue.driver, "Queue");
    }
    if let Ok(driver_str) = std::env::var("METRICS_DRIVER") {
        config.metrics.driver = parse_driver_enum(driver_str, config.metrics.driver, "Metrics");
    }
    if let Ok(driver_str) = std::env::var("APP_MANAGER_DRIVER") {
        config.app_manager.driver =
            parse_driver_enum(driver_str, config.app_manager.driver, "AppManager");
    }
    if let Ok(driver_str) = std::env::var("RATE_LIMITER_DRIVER") {
        config.rate_limiter.driver = parse_driver_enum(
            driver_str,
            config.rate_limiter.driver,
            "RateLimiter Backend",
        );
    }

    if let Ok(val) = std::env::var("SSL_ENABLED") {
        config.ssl.enabled = val == "1" || val.to_lowercase() == "true";
    }
    if let Ok(val) = std::env::var("SSL_CERT_PATH") {
        config.ssl.cert_path = val;
    }
    if let Ok(val) = std::env::var("SSL_KEY_PATH") {
        config.ssl.key_path = val;
    }
    if let Ok(val) = std::env::var("SSL_REDIRECT_HTTP") {
        config.ssl.redirect_http = val == "1" || val.to_lowercase() == "true";
    }
    if let Ok(val_str) = std::env::var("SSL_HTTP_PORT") {
        if let Ok(port) = val_str.parse() {
            config.ssl.http_port = Some(port);
        } else {
            warn!(
                "{}",
                format!("Failed to parse SSL_HTTP_PORT env var: '{}'", val_str)
            );
        }
    }

    if let Ok(redis_url) = std::env::var("REDIS_URL") {
        info!(
            "{}",
            format!("Using Redis URL from environment: {}", redis_url)
        );
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
        config.cache.redis.url_override = Some(redis_url.clone());
        config.queue.redis.url_override = Some(redis_url.clone()); // Set url_override for queue
        config.rate_limiter.redis.url_override = Some(redis_url);
    }
    if let Ok(nats_url) = std::env::var("NATS_URL") {
        info!(
            "{}",
            format!("Using NATS URL from environment: {}", nats_url)
        );
        config.adapter.nats.servers = vec![nats_url];
    }
    // For Redis prefixes from environment variables
    if let Ok(prefix) = std::env::var("CACHE_REDIS_PREFIX") {
        config.cache.redis.prefix = Some(prefix);
    }
    if let Ok(prefix) = std::env::var("QUEUE_REDIS_PREFIX") {
        config.queue.redis.prefix = Some(prefix);
    }
    if let Ok(prefix) = std::env::var("RATE_LIMITER_REDIS_PREFIX") {
        config.rate_limiter.redis.prefix = Some(prefix);
    }

    if let Ok(val) = std::env::var("METRICS_ENABLED") {
        config.metrics.enabled = val == "1" || val.to_lowercase() == "true";
    }
    if let Ok(val_str) = std::env::var("METRICS_PORT") {
        if let Ok(port) = val_str.parse() {
            config.metrics.port = port;
        } else {
            warn!(
                "{}",
                format!("Failed to parse METRICS_PORT env var: '{}'", val_str)
            );
        }
    }

    if let Ok(val) = std::env::var("RATE_LIMITER_ENABLED") {
        config.rate_limiter.enabled = val == "1" || val.to_lowercase() == "true";
    }
    if let Ok(val_str) = std::env::var("RATE_LIMITER_API_MAX_REQUESTS") {
        if let Ok(num) = val_str.parse() {
            config.rate_limiter.api_rate_limit.max_requests = num;
        } else {
            warn!(
                "{}",
                format!(
                    "Failed to parse RATE_LIMITER_API_MAX_REQUESTS: '{}'",
                    val_str
                )
            );
        }
    }
    if let Ok(val_str) = std::env::var("RATE_LIMITER_API_WINDOW_SECONDS") {
        if let Ok(num) = val_str.parse() {
            config.rate_limiter.api_rate_limit.window_seconds = num;
        } else {
            warn!(
                "{}",
                format!(
                    "Failed to parse RATE_LIMITER_API_WINDOW_SECONDS: '{}'",
                    val_str
                )
            );
        }
    }
    if let Ok(val_str) = std::env::var("RATE_LIMITER_API_TRUST_HOPS") {
        if let Ok(num) = val_str.parse() {
            config.rate_limiter.api_rate_limit.trust_hops = Some(num);
        } else {
            warn!(
                "{}",
                format!("Failed to parse RATE_LIMITER_API_TRUST_HOPS: '{}'", val_str)
            );
        }
    }

    let config_path =
        std::env::var("CONFIG_FILE").unwrap_or_else(|_| "src/config.json".to_string());
    if Path::new(&config_path).exists() {
        info!(
            "{}",
            format!("Loading configuration from file: {}", config_path)
        );
        let mut file = File::open(&config_path).map_err(|e| {
            Error::ConfigFileError(format!("Failed to open {}: {}", config_path, e))
        })?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(|e| {
            Error::ConfigFileError(format!("Failed to read {}: {}", config_path, e))
        })?;

        match from_str::<ServerOptions>(&contents) {
            Ok(file_config) => {
                config = file_config;
                info!(
                    "{}",
                    format!(
                        "Successfully loaded and applied configuration from {}",
                        config_path
                    )
                );
            }
            Err(e) => {
                error!("{}", format!("Failed to parse configuration file {}: {}. Using defaults and environment variables.", config_path, e));
            }
        }
    } else {
        info!(
            "{}",
            format!(
                "No configuration file found at {}, using defaults and environment variables.",
                config_path
            )
        );
    }

    let final_debug_enabled = config.debug;
    info!(
        "{}",
        "Final configuration loaded. Initializing server components.".to_string()
    );

    let server = match SockudoServer::new(config).await {
        Ok(s) => s,
        Err(e) => {
            error!("{}", format!("Failed to create server: {}", e));
            return Err(e);
        }
    };

    info!("{}", "Starting Sockudo server services...".to_string());
    if let Err(e) = server.start().await {
        error!("{}", format!("Server runtime error: {}", e));
        return Err(e);
    }

    info!("{}", "Server shutdown complete.".to_string());
    Ok(())
}

fn make_https(host: &str, uri: Uri, https_port: u16) -> core::result::Result<Uri, BoxError> {
    let mut parts = uri.into_parts();
    parts.scheme = Some(axum::http::uri::Scheme::HTTPS);
    if parts.path_and_query.is_none() {
        parts.path_and_query = Some("/".parse().unwrap());
    }
    let authority_val: Authority = host.parse()?;
    let bare_host_str = match authority_val.port() {
        Some(port_struct) => authority_val
            .as_str()
            .strip_suffix(&format!(":{}", port_struct))
            .unwrap_or(authority_val.as_str()),
        None => authority_val.as_str(),
    };
    parts.authority = Some(format!("{}:{}", bare_host_str, https_port).parse()?);
    Uri::from_parts(parts).map_err(Into::into)
}
