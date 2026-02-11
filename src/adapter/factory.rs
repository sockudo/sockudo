use std::sync::Arc;
// src/adapter/factory.rs
use crate::adapter::ConnectionManager;
use crate::adapter::local_adapter::LocalAdapter;
#[cfg(feature = "nats")]
use crate::adapter::nats_adapter::NatsAdapter;
#[cfg(feature = "redis")]
use crate::adapter::redis_adapter::{RedisAdapter, RedisAdapterOptions};
#[cfg(feature = "redis-cluster")]
use crate::adapter::redis_cluster_adapter::RedisClusterAdapter;
use crate::error::Result;

#[cfg(feature = "nats")]
use crate::options::NatsAdapterConfig;
#[cfg(feature = "redis-cluster")]
use crate::options::RedisClusterAdapterConfig; // Import AdapterDriver, RedisConnection
use crate::options::{AdapterConfig, AdapterDriver, DatabaseConfig};
use tracing::{info, warn};

/// Typed adapter enum for direct configuration without downcasting
/// This allows configuring adapters without going through the trait object
#[derive(Clone)]
pub enum TypedAdapter {
    Local(Arc<LocalAdapter>),
    #[cfg(feature = "redis")]
    Redis(Arc<RedisAdapter>),
    #[cfg(feature = "redis-cluster")]
    RedisCluster(Arc<RedisClusterAdapter>),
    #[cfg(feature = "nats")]
    Nats(Arc<NatsAdapter>),
}

impl TypedAdapter {
    /// Get the local adapter reference (available for all adapter types)
    pub fn local_adapter(&self) -> Arc<LocalAdapter> {
        match self {
            TypedAdapter::Local(adapter) => adapter.clone(),
            #[cfg(feature = "redis")]
            TypedAdapter::Redis(adapter) => adapter.local_adapter.clone(),
            #[cfg(feature = "redis-cluster")]
            TypedAdapter::RedisCluster(adapter) => adapter.local_adapter.clone(),
            #[cfg(feature = "nats")]
            TypedAdapter::Nats(adapter) => adapter.local_adapter.clone(),
        }
    }

    /// Set delta compression on the adapter
    /// For horizontal adapters, this delegates to the internal LocalAdapter (which uses OnceLock)
    pub async fn set_delta_compression(
        &self,
        delta_compression: Arc<crate::delta_compression::DeltaCompressionManager>,
        app_manager: Arc<dyn crate::app::manager::AppManager + Send + Sync>,
    ) {
        // All adapters delegate to their internal LocalAdapter which uses OnceLock for thread-safe set-once
        self.local_adapter()
            .set_delta_compression(delta_compression, app_manager)
            .await;
    }

    /// Set tag filtering enabled flag
    pub fn set_tag_filtering_enabled(&self, enabled: bool) {
        self.local_adapter().set_tag_filtering_enabled(enabled);
    }

    /// Set global enable_tags flag
    pub fn set_enable_tags_globally(&self, enabled: bool) {
        self.local_adapter().set_enable_tags_globally(enabled);
    }

    /// Set metrics on the adapter (only applicable to horizontal adapters)
    /// Note: This uses interior mutability through the adapter's internal RwLock
    #[allow(unused_variables)]
    pub async fn set_metrics(
        &self,
        metrics: Arc<tokio::sync::Mutex<dyn crate::metrics::MetricsInterface + Send + Sync>>,
    ) -> Result<()> {
        match self {
            TypedAdapter::Local(_) => {
                // LocalAdapter doesn't have set_metrics
                Ok(())
            }
            #[cfg(feature = "redis")]
            TypedAdapter::Redis(adapter) => adapter.set_metrics(metrics).await,
            #[cfg(feature = "redis-cluster")]
            TypedAdapter::RedisCluster(adapter) => adapter.set_metrics(metrics).await,
            #[cfg(feature = "nats")]
            TypedAdapter::Nats(adapter) => adapter.set_metrics(metrics).await,
        }
    }
}

pub struct AdapterFactory;

impl AdapterFactory {
    /// Create a connection manager without the Mutex wrapper
    /// Use this for lock-free runtime access (all trait methods are &self)
    #[allow(unused_variables)]
    pub async fn create(
        config: &AdapterConfig,
        db_config: &DatabaseConfig,
    ) -> Result<Arc<dyn ConnectionManager + Send + Sync>> {
        Self::create_with_typed(config, db_config)
            .await
            .map(|(adapter, _)| adapter)
    }

    /// Create a connection manager with typed adapter for configuration
    /// Returns:
    /// - Arc<dyn ConnectionManager> for runtime use (lock-free, all methods are &self)
    /// - TypedAdapter for configuration (set_metrics, set_delta_compression, etc.)
    #[allow(unused_variables)]
    pub async fn create_with_typed(
        config: &AdapterConfig,
        db_config: &DatabaseConfig,
    ) -> Result<(Arc<dyn ConnectionManager + Send + Sync>, TypedAdapter)> {
        info!(
            "{}",
            format!(
                "Initializing ConnectionManager with driver: {:?}",
                config.driver
            )
        );
        match config.driver {
            // Match on the enum
            #[cfg(feature = "redis")]
            AdapterDriver::Redis => {
                let redis_url = config
                    .redis
                    .redis_pub_options
                    .get("url")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .unwrap_or_else(|| {
                        format!("redis://{}:{}", db_config.redis.host, db_config.redis.port)
                    });

                let adapter_options = RedisAdapterOptions {
                    url: redis_url,
                    prefix: config.redis.prefix.clone(),
                    request_timeout_ms: config.redis.requests_timeout,
                    cluster_mode: config.redis.cluster_mode,
                };
                match RedisAdapter::new(adapter_options).await {
                    Ok(mut adapter) => {
                        adapter.set_cluster_health(&config.cluster_health).await?;
                        adapter.set_socket_counting(config.enable_socket_counting);
                        let adapter = Arc::new(adapter);
                        let typed = TypedAdapter::Redis(adapter.clone());
                        Ok((adapter, typed))
                    }
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                                "Failed to initialize Redis adapter: {}, falling back to local adapter",
                                e
                            )
                        );
                        let local_adapter = Arc::new(LocalAdapter::new_with_buffer_multiplier(
                            config.buffer_multiplier_per_cpu,
                        ));
                        let typed = TypedAdapter::Local(local_adapter.clone());
                        Ok((local_adapter, typed))
                    }
                }
            }
            #[cfg(feature = "redis-cluster")]
            AdapterDriver::RedisCluster => {
                // Use nodes from the specific RedisClusterAdapterConfig if available, else from DatabaseConfig.redis
                let nodes = if !config.cluster.nodes.is_empty() {
                    config.cluster.nodes.clone()
                } else {
                    db_config
                        .redis
                        .cluster_nodes
                        .iter()
                        .map(|node| node.to_url())
                        .collect()
                };

                if nodes.is_empty() {
                    warn!("{}", "Redis Cluster Adapter selected, but no nodes configured. Falling back to local adapter.".to_string());
                    let local_adapter = Arc::new(LocalAdapter::new_with_buffer_multiplier(
                        config.buffer_multiplier_per_cpu,
                    ));
                    let typed = TypedAdapter::Local(local_adapter.clone());
                    return Ok((local_adapter, typed));
                }

                let cluster_adapter_config = RedisClusterAdapterConfig {
                    // Use the specific config struct
                    nodes,
                    prefix: config.cluster.prefix.clone(),
                    request_timeout_ms: config.cluster.request_timeout_ms,
                    use_connection_manager: config.cluster.use_connection_manager,
                    use_sharded_pubsub: config.cluster.use_sharded_pubsub,
                };
                match RedisClusterAdapter::new(cluster_adapter_config).await {
                    Ok(mut adapter) => {
                        adapter.set_cluster_health(&config.cluster_health).await?;
                        adapter.set_socket_counting(config.enable_socket_counting);
                        let adapter = Arc::new(adapter);
                        let typed = TypedAdapter::RedisCluster(adapter.clone());
                        Ok((adapter, typed))
                    }
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                                "Failed to initialize Redis Cluster adapter: {}, falling back to local adapter",
                                e
                            )
                        );
                        let local_adapter = Arc::new(LocalAdapter::new_with_buffer_multiplier(
                            config.buffer_multiplier_per_cpu,
                        ));
                        let typed = TypedAdapter::Local(local_adapter.clone());
                        Ok((local_adapter, typed))
                    }
                }
            }
            #[cfg(feature = "nats")]
            AdapterDriver::Nats => {
                let nats_cfg = NatsAdapterConfig {
                    // Directly use the NatsAdapterConfig from options
                    servers: config.nats.servers.clone(),
                    prefix: config.nats.prefix.clone(),
                    request_timeout_ms: config.nats.request_timeout_ms,
                    username: config.nats.username.clone(),
                    password: config.nats.password.clone(),
                    token: config.nats.token.clone(),
                    connection_timeout_ms: config.nats.connection_timeout_ms,
                    nodes_number: config.nats.nodes_number,
                };
                match NatsAdapter::new(nats_cfg).await {
                    Ok(mut adapter) => {
                        adapter.set_cluster_health(&config.cluster_health).await?;
                        adapter.set_socket_counting(config.enable_socket_counting);
                        let adapter = Arc::new(adapter);
                        let typed = TypedAdapter::Nats(adapter.clone());
                        Ok((adapter, typed))
                    }
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                                "Failed to initialize NATS adapter: {}, falling back to local adapter",
                                e
                            )
                        );
                        let local_adapter = Arc::new(LocalAdapter::new_with_buffer_multiplier(
                            config.buffer_multiplier_per_cpu,
                        ));
                        let typed = TypedAdapter::Local(local_adapter.clone());
                        Ok((local_adapter, typed))
                    }
                }
            }
            AdapterDriver::Local => {
                info!("{}", "Using local adapter.".to_string());
                let local_adapter = Arc::new(LocalAdapter::new_with_buffer_multiplier(
                    config.buffer_multiplier_per_cpu,
                ));
                let typed = TypedAdapter::Local(local_adapter.clone());
                Ok((local_adapter, typed))
            }
            #[cfg(not(feature = "redis"))]
            AdapterDriver::Redis => {
                warn!(
                    "{}",
                    "Redis adapter requested but not compiled in. Falling back to local adapter."
                        .to_string()
                );
                let local_adapter = Arc::new(LocalAdapter::new_with_buffer_multiplier(
                    config.buffer_multiplier_per_cpu,
                ));
                let typed = TypedAdapter::Local(local_adapter.clone());
                Ok((local_adapter, typed))
            }
            #[cfg(not(feature = "redis-cluster"))]
            AdapterDriver::RedisCluster => {
                warn!("{}", "Redis Cluster adapter requested but not compiled in. Falling back to local adapter.".to_string());
                let local_adapter = Arc::new(LocalAdapter::new_with_buffer_multiplier(
                    config.buffer_multiplier_per_cpu,
                ));
                let typed = TypedAdapter::Local(local_adapter.clone());
                Ok((local_adapter, typed))
            }
            #[cfg(not(feature = "nats"))]
            AdapterDriver::Nats => {
                warn!(
                    "{}",
                    "NATS adapter requested but not compiled in. Falling back to local adapter."
                        .to_string()
                );
                let local_adapter = Arc::new(LocalAdapter::new_with_buffer_multiplier(
                    config.buffer_multiplier_per_cpu,
                ));
                let typed = TypedAdapter::Local(local_adapter.clone());
                Ok((local_adapter, typed))
            }
        }
    }
}
