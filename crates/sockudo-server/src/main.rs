use async_trait::async_trait;
use clap::Parser;
use sockudo_config::drivers::{AdapterDriver, AppManagerDriver, QueueDriver};
use sockudo_core::app_store::AppStore;
use sockudo_runtime::adapter::ConnectionManager;
use sockudo_runtime::app::config::App;
use sockudo_runtime::app::manager::AppManager;
use sockudo_runtime::app::memory_app_manager::MemoryAppManager;
use sockudo_runtime::cache::CacheManagerFactory;
use sockudo_runtime::cache::manager::CacheManager;
use sockudo_runtime::error::{Error, Result};
use sockudo_runtime::metrics::MetricsFactory;
use sockudo_runtime::metrics::MetricsInterface;
use sockudo_runtime::options::ServerOptions;
use sockudo_runtime::queue::manager::{QueueManager, QueueManagerFactory};
use sockudo_runtime::rate_limiter::RateLimiter;
use sockudo_runtime::rate_limiter::factory::RateLimiterFactory;
use sockudo_runtime::webhook::integration::{BatchingConfig, WebhookConfig, WebhookIntegration};
use sonic_rs::JsonValueTrait;
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(version, about = "Sockudo modular server assembly")]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
}

struct ComposedRuntime {
    config: ServerOptions,
    app_manager: Arc<dyn AppManager + Send + Sync>,
    connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    queue_manager: Option<Arc<QueueManager>>,
    webhook_integration: Arc<WebhookIntegration>,
    metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
    http_api_rate_limiter: Arc<dyn RateLimiter + Send + Sync>,
}

struct AppStoreManagerAdapter<T: AppStore + Send + Sync + 'static> {
    inner: T,
}

impl<T: AppStore + Send + Sync + 'static> AppStoreManagerAdapter<T> {
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

fn map_store_error<E: Display>(error: E) -> Error {
    Error::Internal(format!("App store error: {error}"))
}

fn adapt_app_store<T>(store: T) -> Arc<dyn AppManager + Send + Sync>
where
    T: AppStore + Send + Sync + 'static,
    T::Error: Display + Send + Sync + 'static,
{
    Arc::new(AppStoreManagerAdapter::new(store))
}

#[async_trait]
impl<T> AppManager for AppStoreManagerAdapter<T>
where
    T: AppStore + Send + Sync + 'static,
    T::Error: Display + Send + Sync + 'static,
{
    async fn init(&self) -> Result<()> {
        self.inner.init().await.map_err(map_store_error)
    }

    async fn create_app(&self, config: App) -> Result<()> {
        self.inner.create_app(config).await.map_err(map_store_error)
    }

    async fn update_app(&self, config: App) -> Result<()> {
        self.inner.update_app(config).await.map_err(map_store_error)
    }

    async fn delete_app(&self, app_id: &str) -> Result<()> {
        self.inner.delete_app(app_id).await.map_err(map_store_error)
    }

    async fn get_apps(&self) -> Result<Vec<App>> {
        self.inner.get_apps().await.map_err(map_store_error)
    }

    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        self.inner.find_by_key(key).await.map_err(map_store_error)
    }

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        self.inner.find_by_id(app_id).await.map_err(map_store_error)
    }

    async fn check_health(&self) -> Result<()> {
        self.inner.check_health().await.map_err(map_store_error)
    }
}

enum AdapterHandle {
    Local,
    Redis(Arc<sockudo_adapter_redis::RedisAdapterBackend>),
    RedisCluster(Arc<sockudo_adapter_redis_cluster::RedisClusterAdapterBackend>),
    Nats(Arc<sockudo_adapter_nats::NatsAdapterBackend>),
}

impl AdapterHandle {
    async fn set_metrics(
        &self,
        metrics: Arc<Mutex<dyn MetricsInterface + Send + Sync>>,
    ) -> Result<()> {
        match self {
            Self::Local => Ok(()),
            Self::Redis(adapter) => adapter.set_metrics(metrics).await,
            Self::RedisCluster(adapter) => adapter.set_metrics(metrics).await,
            Self::Nats(adapter) => adapter.set_metrics(metrics).await,
        }
    }
}

impl ComposedRuntime {
    async fn build(config: ServerOptions) -> Result<Self> {
        info!("Building modular runtime components...");

        let app_manager = Self::build_app_manager(&config).await?;
        app_manager.init().await?;

        if !config.app_manager.array.apps.is_empty() {
            for app in config
                .app_manager
                .array
                .apps
                .clone()
                .into_iter()
                .map(App::from)
            {
                match app_manager.find_by_id(&app.id).await? {
                    Some(_) => app_manager.update_app(app).await?,
                    None => app_manager.create_app(app).await?,
                }
            }
        }

        let (connection_manager, adapter_handle) = Self::build_connection_manager(&config).await?;
        connection_manager.init().await;

        let cache_manager =
            CacheManagerFactory::create(&config.cache, &config.database.redis).await?;

        let queue_manager = Self::build_queue_manager(&config).await;

        let webhook_config = WebhookConfig {
            enabled: queue_manager.is_some(),
            batching: BatchingConfig {
                enabled: config.webhooks.batching.enabled,
                duration: config.webhooks.batching.duration,
            },
            process_id: config.instance.process_id.clone(),
            debug: config.debug,
        };

        let webhook_integration = Arc::new(
            WebhookIntegration::new(webhook_config, app_manager.clone(), queue_manager.clone())
                .await?,
        );

        let metrics = if config.metrics.enabled {
            MetricsFactory::create(
                config.metrics.driver.as_ref(),
                config.metrics.port,
                Some(config.metrics.prometheus.prefix.as_str()),
            )
            .await
        } else {
            None
        };

        if let Some(metrics_ref) = &metrics {
            let metrics_guard = metrics_ref.lock().await;
            if let Err(e) = metrics_guard.init().await {
                warn!("Metrics initialization failed: {e}");
            }
            if let Err(e) = adapter_handle.set_metrics(Arc::clone(metrics_ref)).await {
                warn!("Unable to attach metrics to adapter: {e}");
            }
        }

        let http_api_rate_limiter =
            RateLimiterFactory::create(&config.rate_limiter, &config.database.redis).await?;

        Ok(Self {
            config,
            app_manager,
            connection_manager,
            cache_manager,
            queue_manager,
            webhook_integration,
            metrics,
            http_api_rate_limiter,
        })
    }

    async fn build_app_manager(
        config: &ServerOptions,
    ) -> Result<Arc<dyn AppManager + Send + Sync>> {
        match config.app_manager.driver {
            AppManagerDriver::Memory => Ok(Arc::new(MemoryAppManager::new())),
            AppManagerDriver::Mysql => {
                let mysql = sockudo_store_mysql::DatabaseConnection {
                    host: config.database.mysql.host.clone(),
                    port: config.database.mysql.port,
                    username: config.database.mysql.username.clone(),
                    password: config.database.mysql.password.clone(),
                    database: config.database.mysql.database.clone(),
                    table_name: config.database.mysql.table_name.clone(),
                    connection_pool_size: config.database.mysql.connection_pool_size,
                    pool_min: config.database.mysql.pool_min,
                    pool_max: config.database.mysql.pool_max,
                    cache_ttl: config.database.mysql.cache_ttl,
                    cache_max_capacity: config.database.mysql.cache_max_capacity,
                };
                let pooling = sockudo_store_mysql::DatabasePooling {
                    enabled: config.database_pooling.enabled,
                    min: config.database_pooling.min,
                    max: config.database_pooling.max,
                };
                match sockudo_store_mysql::new_mysql_store(mysql, pooling).await {
                    Ok(store) => Ok(adapt_app_store(store)),
                    Err(e) => {
                        warn!(
                            "MySQL app manager initialization failed, falling back to memory: {}",
                            e
                        );
                        Ok(Arc::new(MemoryAppManager::new()))
                    }
                }
            }
            AppManagerDriver::PgSql => {
                let postgres = sockudo_store_postgres::DatabaseConnection {
                    host: config.database.postgres.host.clone(),
                    port: config.database.postgres.port,
                    username: config.database.postgres.username.clone(),
                    password: config.database.postgres.password.clone(),
                    database: config.database.postgres.database.clone(),
                    table_name: config.database.postgres.table_name.clone(),
                    connection_pool_size: config.database.postgres.connection_pool_size,
                    pool_min: config.database.postgres.pool_min,
                    pool_max: config.database.postgres.pool_max,
                    cache_ttl: config.database.postgres.cache_ttl,
                    cache_max_capacity: config.database.postgres.cache_max_capacity,
                };
                let pooling = sockudo_store_postgres::DatabasePooling {
                    enabled: config.database_pooling.enabled,
                    min: config.database_pooling.min,
                    max: config.database_pooling.max,
                };
                match sockudo_store_postgres::new_postgres_store(postgres, pooling).await {
                    Ok(store) => Ok(adapt_app_store(store)),
                    Err(e) => {
                        warn!(
                            "PostgreSQL app manager initialization failed, falling back to memory: {}",
                            e
                        );
                        Ok(Arc::new(MemoryAppManager::new()))
                    }
                }
            }
            AppManagerDriver::Dynamodb => {
                let dynamo = sockudo_store_dynamodb::DynamoDbConfig {
                    region: config.database.dynamodb.region.clone(),
                    table_name: config.database.dynamodb.table_name.clone(),
                    endpoint: config.database.dynamodb.endpoint_url.clone(),
                    access_key: config.database.dynamodb.aws_access_key_id.clone(),
                    secret_key: config.database.dynamodb.aws_secret_access_key.clone(),
                    profile_name: config.database.dynamodb.aws_profile_name.clone(),
                };
                match sockudo_store_dynamodb::new_dynamodb_store(dynamo).await {
                    Ok(store) => Ok(adapt_app_store(store)),
                    Err(e) => {
                        warn!(
                            "DynamoDB app manager initialization failed, falling back to memory: {}",
                            e
                        );
                        Ok(Arc::new(MemoryAppManager::new()))
                    }
                }
            }
            AppManagerDriver::ScyllaDb => {
                let scylla = sockudo_store_scylla::ScyllaDbConfig {
                    nodes: config.database.scylladb.nodes.clone(),
                    keyspace: config.database.scylladb.keyspace.clone(),
                    table_name: config.database.scylladb.table_name.clone(),
                    username: config.database.scylladb.username.clone(),
                    password: config.database.scylladb.password.clone(),
                    replication_class: config.database.scylladb.replication_class.clone(),
                    replication_factor: config.database.scylladb.replication_factor,
                };
                match sockudo_store_scylla::new_scylla_store(scylla).await {
                    Ok(store) => Ok(adapt_app_store(store)),
                    Err(e) => {
                        warn!(
                            "ScyllaDB app manager initialization failed, falling back to memory: {}",
                            e
                        );
                        Ok(Arc::new(MemoryAppManager::new()))
                    }
                }
            }
        }
    }

    async fn build_connection_manager(
        config: &ServerOptions,
    ) -> Result<(Arc<dyn ConnectionManager + Send + Sync>, AdapterHandle)> {
        let fallback_local = || {
            let local = Arc::new(
                sockudo_adapter_local::new_local_adapter_with_buffer_multiplier(
                    config.adapter.buffer_multiplier_per_cpu,
                ),
            );
            (
                local.clone() as Arc<dyn ConnectionManager + Send + Sync>,
                AdapterHandle::Local,
            )
        };

        match config.adapter.driver {
            AdapterDriver::Local => Ok(fallback_local()),
            AdapterDriver::Redis => {
                let redis_url = config
                    .adapter
                    .redis
                    .redis_pub_options
                    .get("url")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
                    .unwrap_or_else(|| config.database.redis.to_url());

                let redis_cfg = sockudo_adapter_redis::RedisAdapterOptions {
                    url: redis_url,
                    prefix: config.adapter.redis.prefix.clone(),
                    request_timeout_ms: config.adapter.redis.requests_timeout,
                    cluster_mode: config.adapter.redis.cluster_mode,
                };

                match sockudo_adapter_redis::new_redis_adapter(redis_cfg).await {
                    Ok(mut adapter) => {
                        adapter
                            .set_cluster_health(&config.adapter.cluster_health)
                            .await?;
                        adapter.set_socket_counting(config.adapter.enable_socket_counting);
                        let adapter = Arc::new(adapter);
                        Ok((
                            adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
                            AdapterHandle::Redis(adapter),
                        ))
                    }
                    Err(e) => {
                        warn!("Redis adapter initialization failed, falling back to local: {e}");
                        Ok(fallback_local())
                    }
                }
            }
            AdapterDriver::RedisCluster => {
                let nodes = if !config.adapter.cluster.nodes.is_empty() {
                    config
                        .database
                        .redis
                        .normalize_cluster_seed_urls(&config.adapter.cluster.nodes)
                } else {
                    config.database.redis.cluster_node_urls()
                };

                if nodes.is_empty() {
                    warn!(
                        "Redis Cluster adapter selected but no nodes configured, falling back to local."
                    );
                    return Ok(fallback_local());
                }

                let mut cluster_cfg = config.adapter.cluster.clone();
                cluster_cfg.nodes = nodes;

                match sockudo_adapter_redis_cluster::new_redis_cluster_adapter(cluster_cfg).await {
                    Ok(mut adapter) => {
                        adapter
                            .set_cluster_health(&config.adapter.cluster_health)
                            .await?;
                        adapter.set_socket_counting(config.adapter.enable_socket_counting);
                        let adapter = Arc::new(adapter);
                        Ok((
                            adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
                            AdapterHandle::RedisCluster(adapter),
                        ))
                    }
                    Err(e) => {
                        warn!(
                            "Redis Cluster adapter initialization failed, falling back to local: {e}"
                        );
                        Ok(fallback_local())
                    }
                }
            }
            AdapterDriver::Nats => {
                let nats_cfg = config.adapter.nats.clone();
                match sockudo_adapter_nats::new_nats_adapter(nats_cfg).await {
                    Ok(mut adapter) => {
                        adapter
                            .set_cluster_health(&config.adapter.cluster_health)
                            .await?;
                        adapter.set_socket_counting(config.adapter.enable_socket_counting);
                        let adapter = Arc::new(adapter);
                        Ok((
                            adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
                            AdapterHandle::Nats(adapter),
                        ))
                    }
                    Err(e) => {
                        warn!("NATS adapter initialization failed, falling back to local: {e}");
                        Ok(fallback_local())
                    }
                }
            }
        }
    }

    async fn build_queue_manager(config: &ServerOptions) -> Option<Arc<QueueManager>> {
        if matches!(config.queue.driver, QueueDriver::None) {
            return None;
        }

        let (queue_redis_url_or_nodes, queue_prefix, queue_concurrency) = match &config.queue.driver
        {
            QueueDriver::Redis => (
                Some(
                    config
                        .queue
                        .redis
                        .url_override
                        .clone()
                        .unwrap_or_else(|| config.database.redis.to_url()),
                ),
                config
                    .queue
                    .redis
                    .prefix
                    .as_deref()
                    .unwrap_or("sockudo_queue:"),
                config.queue.redis.concurrency as usize,
            ),
            QueueDriver::RedisCluster => (
                Some(if !config.queue.redis_cluster.nodes.is_empty() {
                    config
                        .database
                        .redis
                        .normalize_cluster_seed_urls(&config.queue.redis_cluster.nodes)
                        .join(",")
                } else {
                    config.database.redis.cluster_node_urls().join(",")
                }),
                config
                    .queue
                    .redis_cluster
                    .prefix
                    .as_deref()
                    .unwrap_or("sockudo_queue:"),
                config.queue.redis_cluster.concurrency as usize,
            ),
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
            Ok(driver) => Some(Arc::new(QueueManager::new(driver))),
            Err(e) => {
                warn!(
                    "Queue manager initialization failed for driver '{:?}': {}",
                    config.queue.driver, e
                );
                None
            }
        }
    }

    async fn check_health(&self) -> Result<()> {
        self.app_manager.check_health().await?;
        self.connection_manager.check_health().await?;

        {
            let cache_guard = self.cache_manager.lock().await;
            cache_guard.check_health().await?;
        }

        if let Some(queue) = &self.queue_manager {
            queue.check_health().await?;
        }

        self.http_api_rate_limiter
            .check("server_health_check")
            .await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        {
            let mut cache_guard = self.cache_manager.lock().await;
            cache_guard.disconnect().await?;
        }

        if let Some(queue) = &self.queue_manager {
            queue.disconnect().await?;
        }

        Ok(())
    }
}

async fn load_server_options(config_path: Option<&str>) -> Result<ServerOptions> {
    let mut options = ServerOptions::default();

    if let Ok(default_config) = ServerOptions::load_from_file("config/config.json").await {
        options = default_config;
    }

    if let Some(path) = config_path {
        options = ServerOptions::load_from_file(path)
            .await
            .map_err(|e| Error::ConfigFile(format!("Failed to load config file '{path}': {e}")))?;
    }

    options
        .override_from_env()
        .await
        .map_err(|e| Error::ConfigFile(format!("Failed to apply environment overrides: {e}")))?;
    options
        .validate()
        .map_err(|e| Error::ConfigFile(format!("Configuration validation failed: {e}")))?;

    Ok(options)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|e| Error::Internal(format!("Failed to install crypto provider: {e:?}")))?;

    let args = Args::parse();
    let config = load_server_options(args.config.as_deref()).await?;

    let runtime = ComposedRuntime::build(config).await?;
    runtime.check_health().await?;

    info!(
        "sockudo-server initialized (host={}, port={}, adapter={:?}, queue={:?})",
        runtime.config.host,
        runtime.config.port,
        runtime.config.adapter.driver,
        runtime.config.queue.driver
    );

    tokio::signal::ctrl_c()
        .await
        .map_err(|e| Error::Internal(format!("Signal listener failed: {e}")))?;

    if runtime.webhook_integration.is_enabled() {
        info!("Shutting down webhook integration");
    }
    if runtime.metrics.is_some() {
        info!("Shutting down metrics integration");
    }

    if let Err(e) = runtime.shutdown().await {
        error!("Error during runtime shutdown: {e}");
    }

    Ok(())
}
