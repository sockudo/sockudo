use async_trait::async_trait;
#[cfg(feature = "redis-cluster")]
use parking_lot::Mutex;
use redis::aio::ConnectionManager;
#[cfg(feature = "redis-cluster")]
use redis::cluster::{ClusterClient, ClusterClientBuilder};
#[cfg(feature = "redis-cluster")]
use redis::cluster_async::ClusterConnection;
#[cfg(feature = "redis-cluster")]
use sockudo_core::error::Error;
use sockudo_core::error::Result;
use sockudo_core::options::SentinelSpec;
use sockudo_core::queue::QueueBackendKind;
use sockudo_core::redis_client::RedisClient;
#[cfg(feature = "redis-cluster")]
use std::sync::Arc;
#[cfg(feature = "redis-cluster")]
use std::time::Duration;

#[async_trait]
pub(crate) trait QueueRedisProvider: Clone + Send + Sync + 'static {
    type Connection: redis::aio::ConnectionLike + Clone + Send + 'static;

    async fn command_connection(&self) -> Result<Self::Connection>;
    async fn worker_connection(&self) -> Result<Self::Connection>;
    fn invalidate(&self);
    fn backend(&self) -> QueueBackendKind;
}

#[derive(Clone)]
pub(crate) struct StandaloneRedisProvider {
    client: RedisClient,
}

impl StandaloneRedisProvider {
    pub(crate) async fn connect(url: &str, sentinel: Option<SentinelSpec>) -> Result<Self> {
        Ok(Self {
            client: RedisClient::connect(url, sentinel).await?,
        })
    }
}

#[async_trait]
impl QueueRedisProvider for StandaloneRedisProvider {
    type Connection = ConnectionManager;

    async fn command_connection(&self) -> Result<Self::Connection> {
        self.client.command_connection().await
    }

    async fn worker_connection(&self) -> Result<Self::Connection> {
        self.client.fresh_connection_manager().await
    }

    fn invalidate(&self) {
        self.client.invalidate();
    }

    fn backend(&self) -> QueueBackendKind {
        if self.client.is_sentinel() {
            QueueBackendKind::RedisSentinel
        } else {
            QueueBackendKind::Redis
        }
    }
}

#[cfg(feature = "redis-cluster")]
struct ClusterInner {
    client: ClusterClient,
    command: Mutex<Option<ClusterConnection>>,
}

#[cfg(feature = "redis-cluster")]
#[derive(Clone)]
pub(crate) struct ClusterRedisProvider {
    inner: Arc<ClusterInner>,
}

#[cfg(feature = "redis-cluster")]
impl ClusterRedisProvider {
    pub(crate) async fn connect(nodes: Vec<String>, request_timeout_ms: u64) -> Result<Self> {
        if nodes.is_empty() {
            return Err(Error::Config(
                "Redis Cluster queue requires at least one seed node".to_string(),
            ));
        }
        let builder = ClusterClientBuilder::new(nodes);
        let builder = if request_timeout_ms == 0 {
            builder.overall_response_timeout(None)
        } else {
            let timeout = Duration::from_millis(request_timeout_ms);
            builder
                .response_timeout(timeout)
                .overall_response_timeout(Some(timeout))
        };
        let client = builder.build().map_err(|error| {
            Error::Config(format!("failed to create Redis Cluster client: {error}"))
        })?;
        let connection = client.get_async_connection().await.map_err(|error| {
            Error::Connection(format!("failed to connect to Redis Cluster: {error}"))
        })?;
        Ok(Self {
            inner: Arc::new(ClusterInner {
                client,
                command: Mutex::new(Some(connection)),
            }),
        })
    }

    async fn build_connection(&self) -> Result<ClusterConnection> {
        self.inner
            .client
            .get_async_connection()
            .await
            .map_err(|error| {
                Error::Connection(format!("failed to connect to Redis Cluster: {error}"))
            })
    }
}

#[cfg(feature = "redis-cluster")]
#[async_trait]
impl QueueRedisProvider for ClusterRedisProvider {
    type Connection = ClusterConnection;

    async fn command_connection(&self) -> Result<Self::Connection> {
        if let Some(connection) = self.inner.command.lock().as_ref() {
            return Ok(connection.clone());
        }
        let connection = self.build_connection().await?;
        *self.inner.command.lock() = Some(connection.clone());
        Ok(connection)
    }

    async fn worker_connection(&self) -> Result<Self::Connection> {
        self.build_connection().await
    }

    fn invalidate(&self) {
        *self.inner.command.lock() = None;
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::RedisCluster
    }
}
