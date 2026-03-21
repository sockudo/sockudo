#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_assignments)]

pub mod cleanup;
mod http_handler;
mod middleware;
mod ws_handler;

#[cfg(unix)]
use axum::extract::connect_info::{self};
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::Method;
use axum::http::header;
use axum::http::header::HeaderName;
use axum::http::uri::Authority;
use axum::http::{HeaderValue, StatusCode, Uri};
use axum::response::Redirect;
use axum::routing::{get, post};
#[cfg(unix)]
use axum::serve::IncomingStream;
use axum::{BoxError, Router, ServiceExt, middleware as axum_middleware};
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use clap::Parser;
use futures_util::future::join_all;
use mimalloc::MiMalloc;
use sockudo_core::error::Error;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::signal;

// Factory imports
use crate::cleanup::{CleanupConfig, CleanupSender};
use crate::http_handler::{
    batch_events, channel, channel_users, channels, events, metrics, terminate_user_connections,
    up, usage,
};
use sockudo_adapter::factory::AdapterFactory;
use sockudo_app::AppManagerFactory;
use sockudo_cache::CacheManagerFactory;
use sockudo_core::error::Result;

use crate::ws_handler::handle_ws_upgrade;
use sockudo_core::options::{QueueDriver, ServerOptions};
use sockudo_core::rate_limiter::RateLimiter;
use sockudo_queue::QueueManagerFactory;
use sockudo_rate_limiter::factory::RateLimiterFactory;
use sockudo_rate_limiter::middleware::IpKeyExtractor;
use sockudo_webhook::integration::QueueManager;
use sockudo_webhook::{BatchingConfig, WebhookConfig, WebhookIntegration};
use tower::Layer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, reload, util::SubscriberInitExt};

// Import concrete adapter types
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::ConnectionManager;

use crate::cleanup::multi_worker::MultiWorkerCleanupSystem;
use crate::middleware::pusher_api_auth_middleware;
use sockudo_cache::MemoryCacheManager;
use sockudo_core::app::AppManager;
use sockudo_core::auth::AuthValidator;
use sockudo_core::cache::CacheManager;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::websocket::WebSocketRef;

#[cfg(unix)]
#[derive(Clone, Debug)]
struct UdsConnectInfo {
    peer_addr: Arc<tokio::net::unix::SocketAddr>,
    peer_cred: tokio::net::unix::UCred,
}

#[cfg(unix)]
/// Convert octal permission mode to human-readable string (e.g., 0o755 -> "rwxr-xr-x")
fn format_permission_string(mode: u32) -> String {
    let owner = [
        (mode & 0o400) != 0,
        (mode & 0o200) != 0,
        (mode & 0o100) != 0,
    ];
    let group = [
        (mode & 0o040) != 0,
        (mode & 0o020) != 0,
        (mode & 0o010) != 0,
    ];
    let other = [
        (mode & 0o004) != 0,
        (mode & 0o002) != 0,
        (mode & 0o001) != 0,
    ];

    [owner, group, other]
        .iter()
        .map(|perms| {
            format!(
                "{}{}{}",
                if perms[0] { 'r' } else { '-' },
                if perms[1] { 'w' } else { '-' },
                if perms[2] { 'x' } else { '-' }
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

#[cfg(unix)]
impl connect_info::Connected<IncomingStream<'_, UnixListener>> for UdsConnectInfo {
    fn connect_info(stream: IncomingStream<'_, UnixListener>) -> Self {
        let peer_addr = stream
            .io()
            .peer_addr()
            .expect("Failed to get peer address from Unix socket");
        let peer_cred = stream
            .io()
            .peer_cred()
            .expect("Failed to get peer credentials from Unix socket");
        Self {
            peer_addr: Arc::new(peer_addr),
            peer_cred,
        }
    }
}

/// Factory for creating metrics instances
pub struct MetricsFactory;

impl MetricsFactory {
    /// Create a new metrics driver based on the specified driver type
    pub async fn create(
        driver_type: &str,
        port: u16,
        prefix: Option<&str>,
    ) -> Option<Arc<dyn MetricsInterface + Send + Sync>> {
        match driver_type.to_lowercase().as_str() {
            "prometheus" => {
                let driver = sockudo_metrics::PrometheusMetricsDriver::new(port, prefix).await;
                Some(Arc::new(driver))
            }
            _ => None,
        }
    }
}

/// Server state containing all managers
struct ServerState {
    app_manager: Arc<dyn AppManager + Send + Sync>,
    connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    local_adapter: Option<Arc<sockudo_adapter::local_adapter::LocalAdapter>>,
    auth_validator: Arc<AuthValidator>,
    cache_manager: Arc<dyn CacheManager + Send + Sync>,
    queue_manager: Option<Arc<QueueManager>>,
    webhooks_integration: Arc<WebhookIntegration>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    running: AtomicBool,
    http_api_rate_limiter: Option<Arc<dyn RateLimiter + Send + Sync>>,
    websocket_rate_limiter: Option<Arc<dyn RateLimiter + Send + Sync>>,
    debug_enabled: bool,
    cleanup_queue: Option<CleanupSender>,
    cleanup_worker_handles: Option<Vec<tokio::task::JoinHandle<()>>>,
    cleanup_config: CleanupConfig,
    delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
    /// Typed adapter for configuration and runtime type inspection
    typed_adapter: sockudo_adapter::factory::TypedAdapter,
}

/// Main server struct
struct SockudoServer {
    config: ServerOptions,
    state: ServerState,
    handler: Arc<ConnectionHandler>,
}

/// Normalize URI path by removing trailing slashes (except for root "/")
fn normalize_uri_path(path: &str) -> String {
    if path.len() > 1 && path.ends_with('/') {
        path[..path.len() - 1].to_string()
    } else {
        path.to_string()
    }
}

/// Common logic for URI normalization
fn normalize_request_uri<B>(mut req: Request<B>) -> Request<B> {
    let uri = req.uri();
    let normalized_path = normalize_uri_path(uri.path());

    if normalized_path != uri.path() {
        let mut parts = uri.clone().into_parts();
        if let Some(path_and_query) = &parts.path_and_query {
            let query = path_and_query
                .query()
                .map(|q| format!("?{q}"))
                .unwrap_or_default();
            let new_path_and_query = format!("{normalized_path}{query}");
            if let Ok(new_pq) = new_path_and_query.parse() {
                parts.path_and_query = Some(new_pq);
                if let Ok(new_uri) = Uri::from_parts(parts) {
                    *req.uri_mut() = new_uri;
                }
            }
        }
    }
    req
}

/// Request URI rewriter for SSL connections (axum_server with TLS)
fn rewrite_request_uri_ssl(req: Request<hyper::body::Incoming>) -> Request<hyper::body::Incoming> {
    let (mut parts, body) = req.into_parts();
    let normalized_path = normalize_uri_path(parts.uri.path());

    if normalized_path != parts.uri.path()
        && let Some(path_and_query) = &parts.uri.path_and_query()
    {
        let query = path_and_query
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let new_path_and_query = format!("{normalized_path}{query}");
        if let Ok(new_pq) = new_path_and_query.parse() {
            let mut uri_parts = parts.uri.clone().into_parts();
            uri_parts.path_and_query = Some(new_pq);
            if let Ok(new_uri) = Uri::from_parts(uri_parts) {
                parts.uri = new_uri;
            }
        }
    }
    Request::from_parts(parts, body)
}

/// Request URI rewriter for non-SSL connections (regular axum)
fn rewrite_request_uri(req: Request<axum::body::Body>) -> Request<axum::body::Body> {
    normalize_request_uri(req)
}

/// Request URI rewriter for HTTP connections with axum_server (uses hyper::body::Incoming)
fn rewrite_request_uri_http(req: Request<hyper::body::Incoming>) -> Request<hyper::body::Incoming> {
    let (mut parts, body) = req.into_parts();
    let normalized_path = normalize_uri_path(parts.uri.path());

    if normalized_path != parts.uri.path()
        && let Some(path_and_query) = &parts.uri.path_and_query()
    {
        let query = path_and_query
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let new_path_and_query = format!("{normalized_path}{query}");
        if let Ok(new_pq) = new_path_and_query.parse() {
            let mut uri_parts = parts.uri.clone().into_parts();
            uri_parts.path_and_query = Some(new_pq);
            if let Ok(new_uri) = Uri::from_parts(uri_parts) {
                parts.uri = new_uri;
            }
        }
    }
    Request::from_parts(parts, body)
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
}

impl SockudoServer {
    fn build_rate_limit_layer(
        limiter: Arc<dyn RateLimiter + Send + Sync>,
        key_prefix: &str,
        trust_hops: usize,
        config_name: &str,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> sockudo_rate_limiter::middleware::RateLimitLayer<IpKeyExtractor> {
        let options = sockudo_rate_limiter::middleware::RateLimitOptions {
            include_headers: true,
            fail_open: false,
            key_prefix: Some(key_prefix.to_string()),
        };

        let mut rate_limit_layer = sockudo_rate_limiter::middleware::RateLimitLayer::with_options(
            limiter,
            IpKeyExtractor::new(trust_hops),
            options,
        )
        .with_config_name(config_name.to_string());

        if let Some(metrics) = metrics {
            rate_limit_layer = rate_limit_layer.with_metrics(metrics.clone());
        }

        rate_limit_layer
    }

    async fn get_http_addr(&self) -> SocketAddr {
        sockudo_core::utils::resolve_socket_addr(&self.config.host, self.config.port, "HTTP server")
            .await
    }

    async fn get_metrics_addr(&self) -> SocketAddr {
        sockudo_core::utils::resolve_socket_addr(
            &self.config.metrics.host,
            self.config.metrics.port,
            "Metrics server",
        )
        .await
    }

    async fn new(config: ServerOptions) -> Result<Self> {
        let debug_enabled = config.debug;
        info!(
            "Initializing Sockudo server with new configuration... Debug mode: {}",
            debug_enabled
        );

        let cache_manager = CacheManagerFactory::create(&config.cache, &config.database.redis)
            .await
            .unwrap_or_else(|e| {
                warn!(
                    "CacheManagerFactory creation failed: {}. Using a NoOp (Memory) Cache.",
                    e
                );
                let fallback_cache_options = config.cache.memory.clone();
                Arc::new(MemoryCacheManager::new(
                    "fallback_cache".to_string(),
                    fallback_cache_options,
                ))
            });
        info!(
            "CacheManager initialized with driver: {:?}",
            config.cache.driver
        );

        let app_manager = AppManagerFactory::create(
            &config.app_manager,
            &config.database,
            &config.database_pooling,
            cache_manager.clone(),
        )
        .await?;
        info!(
            "AppManager initialized with driver: {:?} (cache: {})",
            config.app_manager.driver, config.app_manager.cache.enabled
        );

        let (connection_manager, typed_adapter) =
            AdapterFactory::create_with_typed(&config.adapter, &config.database).await?;
        let local_adapter = Some(typed_adapter.local_adapter());

        info!(
            "Adapter initialized with driver: {:?}",
            config.adapter.driver
        );

        // Set up dead node cleanup event bus for horizontal adapters (only if cluster health is enabled)
        let dead_node_event_receiver = if config.adapter.cluster_health.enabled {
            let receiver_opt = connection_manager.configure_dead_node_events();

            if receiver_opt.is_some() {
                info!(
                    "Event bus configured for clustering adapter: {:?}",
                    config.adapter.driver
                );
            } else {
                info!(
                    "Adapter {:?} doesn't require dead node cleanup events",
                    config.adapter.driver
                );
            }

            receiver_opt
        } else {
            info!("Cluster health disabled, skipping dead node cleanup event bus setup");
            None
        };

        let auth_validator = Arc::new(AuthValidator::new(app_manager.clone()));

        let metrics = if config.metrics.enabled {
            info!(
                "Initializing metrics with driver: {:?}",
                config.metrics.driver
            );
            match MetricsFactory::create(
                config.metrics.driver.as_ref(),
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

        let http_api_rate_limiter_instance = if config.rate_limiter.enabled {
            RateLimiterFactory::create_api(&config.rate_limiter, &config.database.redis)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "Failed to initialize HTTP API rate limiter: {}. Using a permissive limiter.",
                        e
                    );
                    Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(
                        u32::MAX, 1,
                    ))
                })
        } else {
            info!("HTTP API Rate limiting is globally disabled. Using a permissive limiter.");
            Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(u32::MAX, 1))
        };
        info!(
            "HTTP API RateLimiter initialized (enabled: {}) with driver: {:?}",
            config.rate_limiter.enabled, config.rate_limiter.driver
        );

        let websocket_rate_limiter_instance = if config.rate_limiter.enabled {
            RateLimiterFactory::create_websocket(&config.rate_limiter, &config.database.redis)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "Failed to initialize WebSocket rate limiter: {}. Using a permissive limiter.",
                        e
                    );
                    Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(
                        u32::MAX, 1,
                    ))
                })
        } else {
            info!("WebSocket rate limiting is globally disabled. Using a permissive limiter.");
            Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(u32::MAX, 1))
        };
        info!(
            "WebSocket RateLimiter initialized (enabled: {}) with driver: {:?}",
            config.rate_limiter.enabled, config.rate_limiter.driver
        );

        let owned_default_queue_redis_url: String;
        let queue_redis_url_arg: Option<&str>;

        if let Some(url_override) = config.queue.redis.url_override.as_ref() {
            queue_redis_url_arg = Some(url_override.as_str());
        } else {
            owned_default_queue_redis_url = config.database.redis.to_url();
            queue_redis_url_arg = Some(&owned_default_queue_redis_url);
        }

        let queue_manager_opt = match config.queue.driver {
            QueueDriver::Sqs => {
                match QueueManagerFactory::create_sqs(config.queue.sqs.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with SQS driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize SQS queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::Sns => {
                match QueueManagerFactory::create_sns(config.queue.sns.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with SNS driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize SNS queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::None => {
                info!("Queue driver set to None, queue manager will be disabled.");
                None
            }
            QueueDriver::Redis | QueueDriver::RedisCluster | QueueDriver::Memory => {
                let (queue_redis_url_or_nodes, queue_prefix, queue_concurrency) =
                    match config.queue.driver {
                        QueueDriver::Redis => {
                            let owned_default_queue_redis_url: String;
                            let queue_redis_url_arg: Option<&str>;

                            if let Some(url_override) = config.queue.redis.url_override.as_ref() {
                                queue_redis_url_arg = Some(url_override.as_str());
                            } else {
                                owned_default_queue_redis_url = config.database.redis.to_url();
                                queue_redis_url_arg = Some(&owned_default_queue_redis_url);
                            }

                            (
                                queue_redis_url_arg.map(|s| s.to_string()),
                                config
                                    .queue
                                    .redis
                                    .prefix
                                    .as_deref()
                                    .unwrap_or("sockudo_queue:"),
                                config.queue.redis.concurrency as usize,
                            )
                        }
                        QueueDriver::RedisCluster => {
                            let cluster_nodes = if config.queue.redis_cluster.nodes.is_empty() {
                                vec![
                                    "redis://127.0.0.1:7000".to_string(),
                                    "redis://127.0.0.1:7001".to_string(),
                                    "redis://127.0.0.1:7002".to_string(),
                                ]
                            } else {
                                config.queue.redis_cluster.nodes.clone()
                            };

                            let nodes_str = cluster_nodes.join(",");

                            (
                                Some(nodes_str),
                                config
                                    .queue
                                    .redis_cluster
                                    .prefix
                                    .as_deref()
                                    .unwrap_or("sockudo_queue:"),
                                config.queue.redis_cluster.concurrency as usize,
                            )
                        }
                        _ => (None, "sockudo_queue:", 5),
                    };

                match QueueManagerFactory::create(
                    config.queue.driver.as_ref(),
                    queue_redis_url_or_nodes.as_deref(),
                    Some(queue_prefix),
                    Some(queue_concurrency),
                )
                .await
                {
                    Ok(queue_driver_impl) => {
                        info!(
                            "Queue manager initialized with driver: {:?}",
                            config.queue.driver
                        );
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize queue manager with driver '{:?}': {}, queues will be disabled",
                            config.queue.driver, e
                        );
                        None
                    }
                }
            }
        };

        let webhook_config_for_integration = WebhookConfig {
            enabled: queue_manager_opt.is_some(),
            batching: BatchingConfig {
                enabled: config.webhooks.batching.enabled,
                duration: config.webhooks.batching.duration,
                size: config.webhooks.batching.size,
            },
            process_id: config.instance.process_id.clone(),
            debug: config.debug,
        };

        let webhook_integration = match WebhookIntegration::new(
            webhook_config_for_integration,
            app_manager.clone(),
            queue_manager_opt.clone(),
        )
        .await
        {
            Ok(integration) => {
                if integration.is_enabled() {
                    info!("Webhook integration initialized successfully with queue manager");
                } else if queue_manager_opt.is_none() {
                    info!("Webhooks disabled (no queue manager available)");
                } else {
                    info!("Webhook integration initialized (disabled)");
                }
                Arc::new(integration)
            }
            Err(e) => {
                warn!(
                    "Failed to initialize webhook integration: {}, creating disabled instance",
                    e
                );
                let disabled_config = WebhookConfig {
                    enabled: false,
                    ..Default::default()
                };
                Arc::new(WebhookIntegration::new(disabled_config, app_manager.clone(), None).await?)
            }
        };

        // Initialize cleanup queue if enabled
        let cleanup_config = config.cleanup.clone();

        // Validate cleanup configuration
        if let Err(e) = cleanup_config.validate() {
            error!("Invalid cleanup configuration: {}", e);
            return Err(Error::Internal(format!(
                "Invalid cleanup configuration: {}",
                e
            )));
        }

        let (cleanup_queue, cleanup_worker_handles) = if cleanup_config.async_enabled {
            let multi_worker_system = MultiWorkerCleanupSystem::new(
                connection_manager.clone(),
                app_manager.clone(),
                Some(webhook_integration.clone()),
                cleanup_config.clone(),
            );

            let cleanup_sender =
                if let Some(direct_sender) = multi_worker_system.get_direct_sender() {
                    info!("Using direct sender for single worker (optimized)");
                    CleanupSender::Direct(direct_sender)
                } else {
                    info!("Using multi-worker sender with round-robin distribution");
                    CleanupSender::Multi(multi_worker_system.get_sender())
                };

            let worker_handles = multi_worker_system.get_worker_handles();

            info!("Multi-worker cleanup system initialized");
            (Some(cleanup_sender), Some(worker_handles))
        } else {
            (None, None)
        };

        // Initialize delta compression manager
        let algorithm = match config.delta_compression.algorithm.as_str() {
            "xdelta3" => sockudo_delta::DeltaAlgorithm::Xdelta3,
            _ => sockudo_delta::DeltaAlgorithm::Fossil,
        };

        let delta_config = sockudo_delta::DeltaCompressionConfig {
            enabled: config.delta_compression.enabled,
            algorithm,
            full_message_interval: config.delta_compression.full_message_interval,
            min_message_size: config.delta_compression.min_message_size,
            max_state_age: Duration::from_secs(config.delta_compression.max_state_age_secs),
            max_channel_states_per_socket: config.delta_compression.max_channel_states_per_socket,
            min_compression_ratio: 0.9,
            max_conflation_states_per_channel: config
                .delta_compression
                .max_conflation_states_per_channel,
            conflation_key_path: config.delta_compression.conflation_key_path.clone(),
            cluster_coordination: config.delta_compression.cluster_coordination,
            omit_delta_algorithm: config.delta_compression.omit_delta_algorithm,
        };
        let delta_compression_manager = sockudo_delta::DeltaCompressionManager::new(delta_config);

        // Setup cluster coordination if enabled and adapter supports it
        let delta_compression_manager = {
            #[allow(unused_mut)]
            let mut manager = delta_compression_manager;

            if config.delta_compression.cluster_coordination {
                #[cfg(feature = "redis")]
                if matches!(
                    config.adapter.driver,
                    sockudo_core::options::AdapterDriver::Redis
                        | sockudo_core::options::AdapterDriver::RedisCluster
                ) {
                    let redis_url = config.database.redis.to_url();

                    match sockudo_delta::coordination::RedisClusterCoordinator::new(
                        &redis_url,
                        Some(&config.database.redis.key_prefix),
                    )
                    .await
                    {
                        Ok(coordinator) => {
                            info!("Delta compression cluster coordination enabled via Redis");
                            manager.set_cluster_coordinator(Arc::new(coordinator));
                        }
                        Err(e) => {
                            warn!(
                                "Failed to setup Redis cluster coordination, falling back to node-local: {}",
                                e
                            );
                        }
                    }
                }

                #[cfg(feature = "nats")]
                if config.adapter.driver == sockudo_core::options::AdapterDriver::Nats {
                    let nats_servers = config.adapter.nats.servers.clone();

                    match sockudo_delta::coordination::NatsClusterCoordinator::new(
                        nats_servers,
                        Some(&config.adapter.nats.prefix),
                    )
                    .await
                    {
                        Ok(coordinator) => {
                            info!("Delta compression cluster coordination enabled via NATS");
                            manager.set_cluster_coordinator(Arc::new(coordinator));
                        }
                        Err(e) => {
                            warn!(
                                "Failed to setup NATS cluster coordination, falling back to node-local: {}",
                                e
                            );
                        }
                    }
                }
            }

            Arc::new(manager)
        };

        // Start background cleanup task for delta compression state management
        delta_compression_manager.start_cleanup_task().await;

        let state = ServerState {
            app_manager: app_manager.clone(),
            connection_manager: connection_manager.clone(),
            local_adapter: local_adapter.clone(),
            auth_validator,
            cache_manager,
            queue_manager: queue_manager_opt,
            webhooks_integration: webhook_integration.clone(),
            metrics: metrics.clone(),
            running: AtomicBool::new(true),
            http_api_rate_limiter: Some(http_api_rate_limiter_instance.clone()),
            websocket_rate_limiter: Some(websocket_rate_limiter_instance.clone()),
            debug_enabled,
            cleanup_queue,
            cleanup_worker_handles,
            cleanup_config,
            delta_compression: delta_compression_manager.clone(),
            typed_adapter: typed_adapter.clone(),
        };

        let handler = Arc::new(ConnectionHandler::new(
            state.app_manager.clone(),
            state.connection_manager.clone(),
            state.local_adapter.clone(),
            state.cache_manager.clone(),
            state.metrics.clone(),
            Some(webhook_integration),
            config.clone(),
            state.cleanup_queue.clone(),
            delta_compression_manager.clone(),
        ));

        // Start dead node cleanup event processing loop (only runs if cluster health is enabled)
        if let Some(event_receiver) = dead_node_event_receiver {
            let handler_clone = handler.clone();
            tokio::spawn(async move {
                info!("Starting dead node cleanup event processing loop");
                while let Ok(event) = event_receiver.recv().await {
                    if let Err(e) = handler_clone.handle_dead_node_cleanup(event).await {
                        error!("Error processing dead node cleanup event: {}", e);
                    }
                }
                info!("Dead node cleanup event processing loop ended");
            });
        }

        // Set metrics for adapters using TypedAdapter (lock-free configuration)
        if let Some(metrics_instance_arc) = &metrics {
            if let Err(e) = typed_adapter
                .set_metrics(metrics_instance_arc.clone())
                .await
            {
                warn!("Failed to set metrics for adapter: {}", e);
            } else {
                info!(
                    "Metrics configured for adapter: {:?}",
                    config.adapter.driver
                );
            }
        }

        // Set delta compression for adapters using TypedAdapter (lock-free configuration)
        typed_adapter
            .set_delta_compression(delta_compression_manager.clone(), app_manager.clone())
            .await;
        info!(
            "Delta compression initialized for adapter: {:?}",
            config.adapter.driver
        );

        // Set tag filtering enabled flag using TypedAdapter
        typed_adapter.set_tag_filtering_enabled(config.tag_filtering.enabled);
        if config.tag_filtering.enabled {
            info!(
                "Tag filtering enabled for adapter: {:?}",
                config.adapter.driver
            );
        }

        // Set global enable_tags flag using TypedAdapter
        typed_adapter.set_enable_tags_globally(config.tag_filtering.enable_tags);
        info!(
            "Global enable_tags setting: {}",
            config.tag_filtering.enable_tags
        );

        Ok(Self {
            config,
            state,
            handler,
        })
    }

    async fn init(&self) -> Result<()> {
        info!("Server init sequence started.");
        self.state.app_manager.init().await?;

        // Initialize ConnectionManager (Adapter)
        self.state.connection_manager.init().await;

        // Register apps from configuration
        if !self.config.app_manager.array.apps.is_empty() {
            info!(
                "Registering {} apps from configuration",
                self.config.app_manager.array.apps.len()
            );
            let apps_to_register = self.config.app_manager.array.apps.clone();
            for app in apps_to_register {
                info!("Attempting to register app: id={}, key={}", app.id, app.key);
                match self.state.app_manager.find_by_id(&app.id).await {
                    Ok(Some(_existing_app)) => {
                        info!("App {} already exists, attempting to update.", app.id);
                        if let Err(update_err) =
                            self.state.app_manager.update_app(app.clone()).await
                        {
                            error!("Failed to update existing app {}: {}", app.id, update_err);
                        } else {
                            info!("Successfully updated app: {}", app.id);
                        }
                    }
                    Ok(None) => match self.state.app_manager.create_app(app.clone()).await {
                        Ok(_) => info!("Successfully registered new app: {}", app.id),
                        Err(create_err) => {
                            error!("Failed to register new app {}: {}", app.id, create_err)
                        }
                    },
                    Err(e) => {
                        error!(
                            "Error checking existence of app {}: {}. Skipping registration/update.",
                            app.id, e
                        );
                    }
                }
            }
        }

        // Log registered apps
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
            Err(e) => warn!("Failed to retrieve registered apps: {}", e),
        }

        // Initialize Metrics
        if let Some(metrics) = &self.state.metrics
            && let Err(e) = metrics.init().await
        {
            warn!("Failed to initialize metrics: {}", e);
        }
        info!("Server init sequence completed.");
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

        let use_allow_origin_any = self.config.cors.origin.contains(&"*".to_string())
            || self.config.cors.origin.contains(&"Any".to_string())
            || self.config.cors.origin.contains(&"any".to_string());

        if use_allow_origin_any {
            cors_builder = cors_builder.allow_origin(AllowOrigin::any());
            if self.config.cors.credentials {
                warn!(
                    "CORS config: 'Access-Control-Allow-Credentials' was true but 'Access-Control-Allow-Origin' is '*'. Forcing credentials to false to comply with CORS specification."
                );
                cors_builder = cors_builder.allow_credentials(false);
            }
            if self.config.cors.origin.len() > 1 {
                warn!(
                    "CORS config: Wildcard '*' or 'Any' is present in origins list along with other specific origins. Wildcard will take precedence, allowing all origins."
                );
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
            cors_builder = cors_builder.allow_credentials(self.config.cors.credentials);
        } else {
            warn!(
                "CORS origins list is empty and no wildcard ('*' or 'Any') is specified. CORS might be highly restrictive or disabled depending on tower-http defaults. Consider setting origins or '*' for AllowOrigin::any()."
            );
            if self.config.cors.credentials {
                warn!(
                    "CORS origins list is empty, and credentials set to true. Forcing credentials to false for safety as no origin is explicitly allowed."
                );
                cors_builder = cors_builder.allow_credentials(false);
            }
        }

        let cors = cors_builder;

        let api_rate_limiter_middleware_layer = if self.config.rate_limiter.enabled {
            if let Some(rate_limiter_instance) = &self.state.http_api_rate_limiter {
                let trust_hops = self
                    .config
                    .rate_limiter
                    .api_rate_limit
                    .trust_hops
                    .unwrap_or(0) as usize;

                info!(
                    "Applying HTTP API rate limiting middleware with trust_hops: {}",
                    trust_hops
                );
                Some(Self::build_rate_limit_layer(
                    rate_limiter_instance.clone(),
                    "api",
                    trust_hops,
                    "api_rate_limit",
                    self.state.metrics.as_ref(),
                ))
            } else {
                warn!(
                    "Rate limiting is enabled in config, but no RateLimiter instance found in server state for HTTP API. Rate limiting will not be applied."
                );
                None
            }
        } else {
            info!("Custom HTTP API Rate limiting is disabled in configuration.");
            None
        };

        let websocket_rate_limiter_middleware_layer = if self.config.rate_limiter.enabled {
            if let Some(rate_limiter_instance) = &self.state.websocket_rate_limiter {
                let trust_hops = self
                    .config
                    .rate_limiter
                    .websocket_rate_limit
                    .trust_hops
                    .unwrap_or(0) as usize;

                info!(
                    "Applying WebSocket rate limiting middleware with trust_hops: {}",
                    trust_hops
                );
                Some(Self::build_rate_limit_layer(
                    rate_limiter_instance.clone(),
                    "websocket_connect",
                    trust_hops,
                    "websocket_rate_limit",
                    self.state.metrics.as_ref(),
                ))
            } else {
                warn!(
                    "Rate limiting is enabled in config, but no RateLimiter instance found for WebSocket upgrades. WebSocket rate limiting will not be applied."
                );
                None
            }
        } else {
            info!("Custom WebSocket rate limiting is disabled in configuration.");
            None
        };

        let body_limit_bytes =
            (self.config.http_api.request_limit_in_mb as usize).saturating_mul(1024 * 1024);
        debug!(
            "Configuring Axum DefaultBodyLimit to {} MB ({} bytes)",
            self.config.http_api.request_limit_in_mb, body_limit_bytes
        );

        let mut websocket_router = Router::new().route("/app/{appKey}", get(handle_ws_upgrade));
        if let Some(middleware) = websocket_rate_limiter_middleware_layer {
            websocket_router = websocket_router.layer(middleware);
        }

        let mut api_router = Router::new()
            .route(
                "/apps/{appId}/events",
                post(events).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/batch_events",
                post(batch_events).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels",
                get(channels).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}",
                get(channel).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/users",
                get(channel_users).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/users/{userId}/terminate_connections",
                post(terminate_user_connections).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            );

        if let Some(middleware) = api_rate_limiter_middleware_layer {
            api_router = api_router.layer(middleware);
        }

        let mut router = Router::new()
            .merge(websocket_router)
            .merge(api_router)
            .route("/up", get(up))
            .route("/up/{appId}", get(up))
            .layer(DefaultBodyLimit::max(body_limit_bytes))
            .layer(cors);

        if self.config.http_api.usage_enabled {
            router = router.route("/usage", get(usage));
        }

        router.with_state(self.handler.clone())
    }

    fn configure_metrics_routes(&self) -> Router {
        Router::new()
            .route("/metrics", get(metrics))
            .with_state(self.handler.clone())
    }

    async fn start(&self) -> Result<()> {
        info!("Starting Sockudo server services (after init)...");

        // Start metrics server (always runs independently)
        if self.config.metrics.enabled {
            let metrics_router = self.configure_metrics_routes();
            let metrics_addr = self.get_metrics_addr().await;

            match TcpListener::bind(metrics_addr).await {
                Ok(metrics_listener) => {
                    info!("Metrics server listening on http://{}", metrics_addr);
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
                Err(e) => {
                    warn!(
                        "Failed to bind metrics server on {}: {}. Metrics will not be available.",
                        metrics_addr, e
                    );
                }
            }
        }

        let http_router = self.configure_http_routes();

        // Choose between Unix socket OR HTTP/HTTPS for main server
        #[cfg(unix)]
        if self.config.unix_socket.enabled {
            return self.start_unix_socket_server(http_router).await;
        }

        // Fail fast if Unix socket is requested on non-Unix platform
        #[cfg(not(unix))]
        if self.config.unix_socket.enabled {
            error!(
                "Unix socket support is only available on Unix-like systems (Linux, macOS, BSD)."
            );
            error!(
                "Please disable unix_socket.enabled in your configuration to use HTTP/HTTPS instead."
            );
            return Err(Error::Configuration(
                "Unix sockets are not supported on this platform (Windows). Please set unix_socket.enabled to false.".to_string()
            ));
        }

        self.start_http_server(http_router).await
    }

    #[cfg(unix)]
    async fn start_unix_socket_server(&self, http_router: Router) -> Result<()> {
        info!(
            "Starting Unix socket server at path: {}",
            self.config.unix_socket.path
        );
        let path = std::path::PathBuf::from(&self.config.unix_socket.path);

        if path.exists() {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    debug!("Removed existing Unix socket file: {}", path.display());
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug!("Unix socket file was already removed: {}", path.display());
                }
                Err(e) => {
                    warn!("Failed to remove existing Unix socket file: {}", e);
                }
            }
        }

        if let Some(parent) = path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!("Failed to create parent directory for Unix socket: {}", e);
                return Err(Error::Internal(format!(
                    "Failed to create Unix socket directory: {}",
                    e
                )));
            }

            if let Err(e) = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o755))
            {
                warn!(
                    "Failed to set secure permissions on Unix socket parent directory: {}",
                    e
                );
            } else {
                info!(
                    "Set secure permissions (755) on Unix socket parent directory: {}",
                    parent.display()
                );
            }
        }

        let uds = UnixListener::bind(&path)
            .map_err(|e| Error::Internal(format!("Failed to bind Unix socket: {}", e)))?;

        if let Err(e) = std::fs::set_permissions(
            &path,
            std::fs::Permissions::from_mode(self.config.unix_socket.permission_mode),
        ) {
            warn!("Failed to set Unix socket permissions: {}", e);
        } else {
            info!(
                "Set Unix socket permissions to {:o} ({})",
                self.config.unix_socket.permission_mode,
                format_permission_string(self.config.unix_socket.permission_mode)
            );
        }

        let middleware = tower::util::MapRequestLayer::new(rewrite_request_uri);
        let router_with_middleware = middleware.layer(http_router);

        info!("Unix socket server listening on: {}", path.display());
        let app = router_with_middleware.into_make_service_with_connect_info::<UdsConnectInfo>();
        let running = &self.state.running;

        tokio::select! {
            result = axum::serve(uds, app) => {
                if let Err(err) = result {
                    error!("Unix socket server error: {}", err);
                }
            }
            _ = self.shutdown_signal() => {
                info!("Shutdown signal received, stopping Unix socket server...");
                running.store(false, Ordering::SeqCst);
            }
        }

        info!("Unix socket server stopped. Initiating final stop sequence.");
        Ok(())
    }

    async fn start_http_server(&self, http_router: Router) -> Result<()> {
        let http_addr = self.get_http_addr().await;

        let middleware = tower::util::MapRequestLayer::new(rewrite_request_uri);
        let router_with_middleware = middleware.layer(http_router.clone());

        let middleware_ssl = tower::util::MapRequestLayer::new(rewrite_request_uri_ssl);
        let router_with_middleware_ssl = middleware_ssl.layer(http_router.clone());

        let middleware_http = tower::util::MapRequestLayer::new(rewrite_request_uri_http);
        let router_with_middleware_http = middleware_http.layer(http_router);

        if self.config.ssl.enabled
            && !self.config.ssl.cert_path.is_empty()
            && !self.config.ssl.key_path.is_empty()
        {
            info!("SSL is enabled, starting HTTPS server");
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
                    "Starting HTTP to HTTPS redirect server on {}",
                    redirect_addr
                );

                let https_port = self.config.port;
                let redirect_app =
                    Router::new().fallback(move |headers: axum::http::HeaderMap, uri: Uri| async move {
                        let host = headers
                            .get(header::HOST)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("localhost");
                        match make_https(host, uri, https_port) {
                            Ok(uri_https) => Ok(Redirect::permanent(&uri_https.to_string())),
                            Err(error) => {
                                error!(error = ?error, "failed to convert URI to HTTPS for redirect");
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
                    Err(e) => warn!(
                        "Failed to bind HTTP redirect server on {}: {}. Redirect will not be available.",
                        redirect_addr, e
                    ),
                }
            }

            info!("HTTPS server listening on https://{}", http_addr);
            let running = &self.state.running;
            let server = axum_server::bind(http_addr).acceptor(
                RustlsAcceptor::new(tls_config)
                    .acceptor(axum_server::accept::NoDelayAcceptor::new()),
            );

            tokio::select! {
                result = server.serve(router_with_middleware_ssl.into_make_service_with_connect_info::<SocketAddr>()) => {
                    if let Err(err) = result {
                        error!("HTTPS server error: {}", err);
                    }
                }
                _ = self.shutdown_signal() => {
                    info!("Shutdown signal received, stopping HTTPS server...");
                    running.store(false, Ordering::SeqCst);
                }
            }
        } else {
            info!("SSL is not enabled, starting HTTP server");
            info!("HTTP server listening on http://{}", http_addr);

            let running = &self.state.running;
            let http_server = axum_server::bind(http_addr)
                .acceptor(axum_server::accept::NoDelayAcceptor::new())
                .serve(
                    router_with_middleware_http.into_make_service_with_connect_info::<SocketAddr>(),
                );

            tokio::select! {
                res = http_server => {
                    if let Err(err) = res {
                        error!("HTTP server error: {}", err);
                    }
                }
                _ = self.shutdown_signal() => {
                    info!("Shutdown signal received, stopping HTTP server...");
                    running.store(false, Ordering::SeqCst);
                }
            }
        }

        info!("HTTP server stopped. Initiating final stop sequence.");
        Ok(())
    }

    async fn load_tls_config(&self) -> Result<RustlsConfig> {
        let cert_path = std::path::PathBuf::from(&self.config.ssl.cert_path);
        let key_path = std::path::PathBuf::from(&self.config.ssl.key_path);
        if !cert_path.exists() {
            return Err(Error::ConfigFile(format!(
                "SSL cert_path not found: {cert_path:?}"
            )));
        }
        if !key_path.exists() {
            return Err(Error::ConfigFile(format!(
                "SSL key_path not found: {key_path:?}"
            )));
        }
        RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| Error::Internal(format!("Failed to load TLS configuration: {e}")))
    }

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
            _ = ctrl_c => info!("Ctrl+C received, initiating shutdown..."),
            _ = terminate => info!("Terminate signal received, initiating shutdown..."),
        }
    }

    async fn stop(&self) -> Result<()> {
        info!("Stopping server...");
        self.state.running.store(false, Ordering::SeqCst);

        let mut connections_to_cleanup: Vec<(String, WebSocketRef)> = Vec::new();

        {
            match self.state.connection_manager.get_namespaces().await {
                Ok(namespaces_vec) => {
                    for (app_id, namespace_obj) in namespaces_vec {
                        match namespace_obj.get_sockets().await {
                            Ok(sockets_vec) => {
                                for (_socket_id, ws_raw_obj) in sockets_vec {
                                    connections_to_cleanup
                                        .push((app_id.clone(), ws_raw_obj.clone()));
                                }
                            }
                            Err(e) => {
                                warn!(%app_id, "Failed to get sockets for namespace during shutdown: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to get namespaces during shutdown: {}", e);
                }
            }
        }

        info!(
            "Collected {} connections to cleanup.",
            connections_to_cleanup.len()
        );

        if !connections_to_cleanup.is_empty() {
            let cleanup_futures =
                connections_to_cleanup
                    .into_iter()
                    .map(|(_app_id, ws_raw_obj)| async move {
                        let mut ws = ws_raw_obj.inner.lock().await;
                        if let Err(e) = ws.close(4200, "Server shutting down".to_string()).await {
                            error!("Failed to close WebSocket: {:?}", e);
                        }
                    });

            join_all(cleanup_futures).await;
            info!("All connection cleanup tasks have been processed.");
        } else {
            info!("No connections to cleanup.");
        }

        if self.state.cleanup_worker_handles.is_some() {
            info!("Cleanup system will shutdown when server process ends");
        }

        // Stop delta compression cleanup task gracefully
        self.state.delta_compression.stop_cleanup_task().await;

        // Disconnect from backend services
        if let Err(e) = self.state.cache_manager.disconnect().await {
            warn!("Error disconnecting cache manager: {}", e);
        }
        if let Some(queue_manager_arc) = &self.state.queue_manager
            && let Err(e) = queue_manager_arc.disconnect().await
        {
            warn!("Error disconnecting queue manager: {}", e);
        }

        // Clean up Unix socket file if it exists (with race condition handling)
        #[cfg(unix)]
        if self.config.unix_socket.enabled {
            let path = std::path::PathBuf::from(&self.config.unix_socket.path);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    info!("Removed Unix socket file: {}", self.config.unix_socket.path);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug!("Unix socket file was already removed: {}", path.display());
                }
                Err(e) => {
                    warn!("Failed to remove Unix socket file during shutdown: {}", e);
                }
            }
        }

        info!(
            "Waiting for shutdown grace period: {} seconds",
            self.config.shutdown_grace_period
        );
        tokio::time::sleep(Duration::from_secs(self.config.shutdown_grace_period)).await;
        info!("Server stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use sockudo_rate_limiter::memory_limiter::MemoryRateLimiter;
    use tower::ServiceExt;

    async fn ok_handler() -> StatusCode {
        StatusCode::OK
    }

    fn scoped_test_router() -> Router {
        let api_layer = SockudoServer::build_rate_limit_layer(
            Arc::new(MemoryRateLimiter::new(1, 60)),
            "api",
            0,
            "api_rate_limit",
            None,
        );
        let websocket_layer = SockudoServer::build_rate_limit_layer(
            Arc::new(MemoryRateLimiter::new(1, 60)),
            "websocket_connect",
            0,
            "websocket_rate_limit",
            None,
        );

        Router::new()
            .merge(
                Router::new()
                    .route("/app/{appKey}", get(ok_handler))
                    .layer(websocket_layer),
            )
            .merge(
                Router::new()
                    .route("/apps/{appId}/events", post(ok_handler))
                    .layer(api_layer),
            )
    }

    #[tokio::test]
    async fn websocket_rate_limit_is_scoped_to_websocket_routes() {
        let app = scoped_test_router();

        let ws_request = Request::builder()
            .method("GET")
            .uri("/app/demo-key")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let ws_response = app.clone().oneshot(ws_request).await.unwrap();
        assert_eq!(ws_response.status(), StatusCode::OK);

        let api_request = Request::builder()
            .method("POST")
            .uri("/apps/demo/events")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let api_response = app.clone().oneshot(api_request).await.unwrap();
        assert_eq!(api_response.status(), StatusCode::OK);

        let second_ws_request = Request::builder()
            .method("GET")
            .uri("/app/demo-key")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let second_ws_response = app.oneshot(second_ws_request).await.unwrap();
        assert_eq!(second_ws_response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn api_rate_limit_is_scoped_to_api_routes() {
        let app = scoped_test_router();

        let api_request = Request::builder()
            .method("POST")
            .uri("/apps/demo/events")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let api_response = app.clone().oneshot(api_request).await.unwrap();
        assert_eq!(api_response.status(), StatusCode::OK);

        let ws_request = Request::builder()
            .method("GET")
            .uri("/app/demo-key")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let ws_response = app.clone().oneshot(ws_request).await.unwrap();
        assert_eq!(ws_response.status(), StatusCode::OK);

        let second_api_request = Request::builder()
            .method("POST")
            .uri("/apps/demo/events")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let second_api_response = app.oneshot(second_api_request).await.unwrap();
        assert_eq!(second_api_response.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize crypto provider at the very beginning for any TLS usage
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|e| {
            Error::Internal(format!("Failed to install default crypto provider: {e:?}"))
        })?;

    // Helper function to get log directive based on debug mode
    fn get_log_directive(is_debug: bool) -> String {
        if is_debug {
            std::env::var("SOCKUDO_LOG_DEBUG")
                .unwrap_or_else(|_| "info,sockudo=debug,tower_http=debug".to_string())
        } else {
            std::env::var("SOCKUDO_LOG_PROD").unwrap_or_else(|_| "info".to_string())
        }
    }

    let initial_debug_from_env = if std::env::var("DEBUG").is_ok() {
        sockudo_core::utils::parse_bool_env("DEBUG", false)
    } else {
        sockudo_core::utils::parse_bool_env("DEBUG_MODE", false)
    };

    let use_json_format = std::env::var("LOG_OUTPUT_FORMAT").as_deref() == Ok("json");

    let (filter_reload_handle, fmt_reload_handle) = if use_json_format {
        let initial_log_directive = get_log_directive(initial_debug_from_env);
        let initial_env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(initial_log_directive));

        tracing_subscriber::fmt()
            .json()
            .with_target(sockudo_core::utils::parse_bool_env(
                "LOG_INCLUDE_TARGET",
                true,
            ))
            .with_file(initial_debug_from_env)
            .with_line_number(initial_debug_from_env)
            .with_env_filter(initial_env_filter)
            .init();

        info!("Initial logging initialized: JSON format");
        (None, None)
    } else {
        let initial_log_directive = get_log_directive(initial_debug_from_env);
        let initial_env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(initial_log_directive));

        let (filter_layer, filter_reload_handle) = reload::Layer::new(initial_env_filter);

        let initial_fmt_layer = fmt::layer()
            .with_target(true)
            .with_file(initial_debug_from_env)
            .with_line_number(initial_debug_from_env);

        let (fmt_layer, fmt_reload_handle) = reload::Layer::new(initial_fmt_layer);

        tracing_subscriber::registry()
            .with(filter_layer)
            .with(fmt_layer)
            .init();

        info!(
            "Initial logging initialized: Human format with DEBUG={}",
            initial_debug_from_env
        );

        (Some(filter_reload_handle), Some(fmt_reload_handle))
    };

    // --- Configuration Loading Order ---
    let mut config = ServerOptions::default();
    info!("Starting with default configuration");

    match ServerOptions::load_from_file("config/config.json").await {
        Ok(file_config) => {
            config = file_config;
            info!("Loaded configuration from config/config.json");
        }
        Err(e) => {
            info!("No config/config.json found or failed to load: {e}. Using defaults.");
        }
    }

    if let Some(config_path) = args.config {
        match ServerOptions::load_from_file(&config_path).await {
            Ok(file_config) => {
                config = file_config;
                info!(
                    "Successfully loaded and applied configuration from {}",
                    config_path
                );
            }
            Err(e) => {
                error!(
                    "Failed to load configuration file {}: {}. Continuing with previously loaded config.",
                    config_path, e
                );
            }
        }
    }

    match config.override_from_env().await {
        Ok(_) => {
            info!("Applied environment variable overrides");
        }
        Err(e) => {
            error!("Failed to override config from environment: {e}");
        }
    }

    if let Err(e) = config.validate() {
        error!("Configuration validation failed: {}", e);
        return Err(Error::ConfigFile(format!(
            "Configuration validation failed: {}",
            e
        )));
    }

    // --- Update logging configuration if needed ---
    if let (Some(filter_handle), Some(fmt_handle)) = (filter_reload_handle, fmt_reload_handle) {
        let needs_logging_update =
            config.debug != initial_debug_from_env || config.logging.is_some();

        if needs_logging_update {
            if config.debug != initial_debug_from_env {
                info!(
                    "Debug mode changed from {} to {} after loading configuration, updating logger",
                    initial_debug_from_env, config.debug
                );
            }

            if config.logging.is_some() {
                info!("Custom logging configuration detected, updating logger format");
            }

            let new_log_directive = get_log_directive(config.debug);
            let new_env_filter = EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(new_log_directive));

            if let Err(e) = filter_handle.reload(new_env_filter) {
                error!("Failed to reload logging filter: {}", e);
            } else {
                debug!(
                    "Successfully updated logging filter for debug={}",
                    config.debug
                );
            }

            let new_fmt_layer = match &config.logging {
                Some(logging_config) => {
                    info!(
                        "Using human-readable log format with colors_enabled={}",
                        logging_config.colors_enabled
                    );
                    fmt::layer()
                        .with_ansi(logging_config.colors_enabled)
                        .with_target(logging_config.include_target)
                        .with_file(config.debug)
                        .with_line_number(config.debug)
                }
                None => fmt::layer()
                    .with_target(true)
                    .with_file(config.debug)
                    .with_line_number(config.debug),
            };

            if let Err(e) = fmt_handle.reload(new_fmt_layer) {
                error!("Failed to reload fmt layer: {}", e);
            } else {
                debug!("Successfully updated fmt layer");
            }
        }
    } else if use_json_format {
        info!("Logging was initialized with JSON format via environment variable");
    }

    info!(
        "Configuration loading complete. Debug mode: {}. Effective RUST_LOG/default filter: '{}'",
        config.debug,
        EnvFilter::try_from_default_env()
            .map(|f| f.to_string())
            .unwrap_or("None".to_string())
    );

    info!("Starting Sockudo server initialization process with resolved configuration...");

    let server = match SockudoServer::new(config).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create server instance: {}", e);
            return Err(e);
        }
    };

    if let Err(e) = server.init().await {
        error!("Failed to initialize server components: {}", e);
        return Err(e);
    }

    info!("Starting Sockudo server main services...");
    if let Err(e) = server.start().await {
        error!("Server runtime error: {}", e);
        if let Err(stop_err) = server.stop().await {
            error!("Error during server stop after runtime error: {}", stop_err);
        }
        return Err(e);
    }

    info!("Server main services concluded. Performing final shutdown...");
    if let Err(e) = server.stop().await {
        error!("Error during final server stop: {}", e);
    }

    info!("Sockudo server shutdown complete.");
    Ok(())
}

fn make_https(host: &str, uri: Uri, https_port: u16) -> core::result::Result<Uri, BoxError> {
    let mut parts = uri.into_parts();
    parts.scheme = Some(http::uri::Scheme::HTTPS);

    if parts.path_and_query.is_none() {
        parts.path_and_query = Some("/".parse().unwrap());
    }

    let authority_val: Authority = host
        .parse()
        .map_err(|e| format!("Failed to parse host '{host}' into authority: {e}"))?;

    let bare_host_str = authority_val.host();

    parts.authority = Some(
        format!("{bare_host_str}:{https_port}")
            .parse()
            .map_err(|e| {
                format!("Failed to create new authority '{bare_host_str}:{https_port}': {e}")
            })?,
    );

    Uri::from_parts(parts).map_err(Into::into)
}
