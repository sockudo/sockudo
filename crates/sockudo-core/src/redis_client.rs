//! Shared standalone and Sentinel-aware Redis connection provider.
//!
//! Command connections are cached and cheap to clone. Worker/listener paths can
//! request a fresh connection so a blocking command never stalls unrelated work.
//!
//! Cluster connections do not retry initial node discovery on their own;
//! use [`cluster_connect_with_retry`] at startup.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use redis::aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection, PubSub};
use redis::sentinel::{SentinelClient, SentinelClientBuilder, SentinelServerType};
use redis::{ClientTlsConfig, ConnectionAddr, IntoConnectionInfo, TlsCertificates, TlsMode};
use tracing::{info, warn};

use crate::error::{Error, Result};
use crate::options::{RedisTlsOptions, SentinelSpec};

/// Maximum number of connection attempts before giving up.
const CONNECT_MAX_RETRIES: usize = 5;

/// Exponential backoff multiplier between retries.
const CONNECT_EXPONENT_BASE: f32 = 2.0;

/// Upper bound on the delay between retries.
const CONNECT_MAX_DELAY: Duration = Duration::from_millis(5_000);

/// Initial delay (ms) for the cluster startup retry loop.
const CLUSTER_CONNECT_BASE_DELAY_MS: u64 = 200;

pub fn connection_manager_config() -> ConnectionManagerConfig {
    ConnectionManagerConfig::new()
        .set_number_of_retries(CONNECT_MAX_RETRIES)
        .set_exponent_base(CONNECT_EXPONENT_BASE)
        .set_max_delay(CONNECT_MAX_DELAY)
}

enum ClientSource {
    Standalone(redis::Client),
    // redis-rs master resolution requires &mut SentinelClient. This lock only
    // serializes the short topology lookup and is never used on command hot paths.
    Sentinel(tokio::sync::Mutex<SentinelClient>),
}

struct Inner {
    source: ClientSource,
    manager_config: ConnectionManagerConfig,
    connection: Mutex<Option<ConnectionManager>>,
    events_connection: Mutex<Option<ConnectionManager>>,
}

/// Cheap-to-clone handle over standalone Redis/rediss or a Sentinel primary.
#[derive(Clone)]
pub struct RedisClient {
    inner: Arc<Inner>,
}

/// Connection settings shared by standalone and Sentinel-backed Redis clients.
#[derive(Debug, Clone, Default)]
pub struct RedisClientOptions {
    /// Native Sentinel topology. When present, `url` is not parsed.
    pub sentinel: Option<SentinelSpec>,
    /// TLS settings for a direct Redis data connection.
    ///
    /// Sentinel clients use the TLS settings embedded in [`SentinelSpec`].
    pub tls: RedisTlsOptions,
    /// Optional command-response timeout for cached connection managers.
    pub response_timeout: Option<Duration>,
}

impl RedisClient {
    /// Wraps an already configured standalone client.
    pub async fn from_client(client: redis::Client) -> Result<Self> {
        Self::from_source(
            ClientSource::Standalone(client),
            connection_manager_config(),
        )
        .await
    }

    /// Connects and eagerly verifies the command path.
    pub async fn connect(url: &str, sentinel: Option<SentinelSpec>) -> Result<Self> {
        Self::connect_with_options(
            url,
            RedisClientOptions {
                sentinel,
                ..Default::default()
            },
        )
        .await
    }

    /// Connects with explicit direct-TLS and timeout settings.
    pub async fn connect_with_options(url: &str, options: RedisClientOptions) -> Result<Self> {
        let manager_config =
            connection_manager_config().set_response_timeout(options.response_timeout);

        let source = match options.sentinel {
            Some(spec) => {
                ClientSource::Sentinel(tokio::sync::Mutex::new(build_sentinel_client(&spec).await?))
            }
            None => ClientSource::Standalone(build_standalone_client(url, &options.tls).await?),
        };

        Self::from_source(source, manager_config).await
    }

    async fn from_source(
        source: ClientSource,
        manager_config: ConnectionManagerConfig,
    ) -> Result<Self> {
        let client = Self {
            inner: Arc::new(Inner {
                source,
                manager_config,
                connection: Mutex::new(None),
                events_connection: Mutex::new(None),
            }),
        };
        let _ = client.command_connection().await?;
        let _ = client.events_connection().await?;
        Ok(client)
    }

    async fn master_client(&self) -> Result<redis::Client> {
        match &self.inner.source {
            ClientSource::Standalone(client) => Ok(client.clone()),
            ClientSource::Sentinel(sentinel) => sentinel
                .lock()
                .await
                .async_get_client()
                .await
                .map_err(|error| {
                    Error::Redis(format!("failed to resolve Redis Sentinel primary: {error}"))
                }),
        }
    }

    async fn build_manager_with_config(
        &self,
        manager_config: ConnectionManagerConfig,
    ) -> Result<ConnectionManager> {
        self.master_client()
            .await?
            .get_connection_manager_with_config(manager_config)
            .await
            .map_err(|error| Error::Redis(format!("failed to connect to Redis: {error}")))
    }

    async fn build_manager(&self) -> Result<ConnectionManager> {
        self.build_manager_with_config(self.inner.manager_config.clone())
            .await
    }

    async fn get_or_build(
        &self,
        slot: &Mutex<Option<ConnectionManager>>,
    ) -> Result<ConnectionManager> {
        if let Some(manager) = slot.lock().as_ref() {
            return Ok(manager.clone());
        }
        let manager = self.build_manager().await?;
        *slot.lock() = Some(manager.clone());
        Ok(manager)
    }

    pub async fn command_connection(&self) -> Result<ConnectionManager> {
        self.get_or_build(&self.inner.connection).await
    }

    pub async fn events_connection(&self) -> Result<ConnectionManager> {
        self.get_or_build(&self.inner.events_connection).await
    }

    /// Returns an independently multiplexed connection suitable for a worker
    /// that may issue a blocking command.
    pub async fn fresh_connection_manager(&self) -> Result<ConnectionManager> {
        self.build_manager().await
    }

    /// Returns an independent connection manager with a caller-specific
    /// response timeout. Blocking-command users can extend the deadline beyond
    /// the server-side wait without changing cached command connections.
    pub async fn fresh_connection_manager_with_response_timeout(
        &self,
        response_timeout: Option<Duration>,
    ) -> Result<ConnectionManager> {
        let manager_config = self
            .inner
            .manager_config
            .clone()
            .set_response_timeout(response_timeout);
        self.build_manager_with_config(manager_config).await
    }

    /// Invalidates cached data-plane connections after a Sentinel failover.
    pub fn invalidate(&self) {
        if matches!(self.inner.source, ClientSource::Sentinel(_)) {
            *self.inner.connection.lock() = None;
            *self.inner.events_connection.lock() = None;
        }
    }

    #[must_use]
    pub fn is_sentinel(&self) -> bool {
        matches!(self.inner.source, ClientSource::Sentinel(_))
    }

    /// Open a fresh RESP3 subscription connection with caller-owned bounded
    /// push admission. Re-resolve Sentinel on every reconnect and retain TLS,
    /// authentication and database settings from the selected primary.
    pub async fn pubsub_with_push_sender(
        &self,
        sender: impl redis::aio::AsyncPushSender,
    ) -> Result<MultiplexedConnection> {
        let client = self.master_client().await?;
        let info = client.get_connection_info().clone();
        let settings = info
            .redis_settings()
            .clone()
            .set_protocol(redis::ProtocolVersion::RESP3);
        let client = redis::Client::open(info.set_redis_settings(settings)).map_err(|error| {
            Error::Redis(format!("failed to prepare Redis push connection: {error}"))
        })?;
        client
            .get_multiplexed_async_connection_with_config(
                &redis::AsyncConnectionConfig::new().set_push_sender(sender),
            )
            .await
            .map_err(|error| Error::Redis(format!("failed to open Redis push connection: {error}")))
    }

    pub async fn pubsub(&self) -> Result<PubSub> {
        self.master_client()
            .await?
            .get_async_pubsub()
            .await
            .map_err(|error| {
                Error::Redis(format!("failed to get Redis pub/sub connection: {error}"))
            })
    }

    pub async fn multiplexed(&self) -> Result<MultiplexedConnection> {
        self.master_client()
            .await?
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| Error::Redis(format!("failed to get Redis connection: {error}")))
    }
}

/// Builds a direct Redis client, applying private-CA and mutual-TLS material
/// when configured. Enabling TLS upgrades a `redis://` address to a TLS socket,
/// so callers do not need to encode certificate behavior in the URL.
pub async fn build_standalone_client(url: &str, tls: &RedisTlsOptions) -> Result<redis::Client> {
    let connection_info = url.into_connection_info().map_err(|error| {
        Error::Redis(format!("failed to parse direct Redis connection: {error}"))
    })?;

    if !tls.enabled {
        return redis::Client::open(connection_info)
            .map_err(|error| Error::Redis(format!("failed to create Redis client: {error}")));
    }

    let tls_addr = match connection_info.addr() {
        ConnectionAddr::Tcp(host, port) => ConnectionAddr::TcpTls {
            host: host.clone(),
            port: *port,
            insecure: tls.accept_invalid_certs,
            tls_params: None,
        },
        ConnectionAddr::TcpTls { host, port, .. } => ConnectionAddr::TcpTls {
            host: host.clone(),
            port: *port,
            insecure: tls.accept_invalid_certs,
            tls_params: None,
        },
        ConnectionAddr::Unix(_) => {
            return Err(Error::Redis(
                "direct Redis TLS cannot be used with a Unix socket".to_string(),
            ));
        }
        _ => {
            return Err(Error::Redis(
                "unsupported direct Redis address for TLS".to_string(),
            ));
        }
    };
    let connection_info = connection_info.set_addr(tls_addr);

    match load_tls_certificates(tls, "direct").await? {
        Some(certificates) => {
            redis::Client::build_with_tls(connection_info, certificates).map_err(|error| {
                Error::Redis(format!("failed to create direct Redis TLS client: {error}"))
            })
        }
        None => redis::Client::open(connection_info).map_err(|error| {
            Error::Redis(format!("failed to create direct Redis TLS client: {error}"))
        }),
    }
}

fn tls_mode(tls: &RedisTlsOptions) -> TlsMode {
    if tls.accept_invalid_certs {
        TlsMode::Insecure
    } else {
        TlsMode::Secure
    }
}

pub async fn load_tls_certificates(
    tls: &RedisTlsOptions,
    hop: &str,
) -> Result<Option<TlsCertificates>> {
    if (tls.client_cert_path.is_some()) ^ (tls.client_key_path.is_some()) {
        warn!(
            hop = %hop,
            "partial redis mutual tls configuration ignored"
        );
    }
    if tls.ca_path.is_none() && !tls.has_client_cert() {
        return Ok(None);
    }

    let root_cert = match &tls.ca_path {
        Some(path) => Some(tokio::fs::read(path).await.map_err(|error| {
            Error::Redis(format!(
                "failed to read Redis {hop} TLS CA certificate {path}: {error}"
            ))
        })?),
        None => None,
    };
    let client_tls = match (&tls.client_cert_path, &tls.client_key_path) {
        (Some(cert_path), Some(key_path)) => Some(ClientTlsConfig {
            client_cert: tokio::fs::read(cert_path).await.map_err(|error| {
                Error::Redis(format!(
                    "failed to read Redis {hop} client certificate {cert_path}: {error}"
                ))
            })?,
            client_key: tokio::fs::read(key_path).await.map_err(|error| {
                Error::Redis(format!(
                    "failed to read Redis {hop} client key {key_path}: {error}"
                ))
            })?,
        }),
        _ => None,
    };
    Ok(Some(TlsCertificates {
        client_tls,
        root_cert,
    }))
}

/// Applies shared Redis data-plane TLS settings to a cluster client builder.
pub async fn configure_cluster_builder(
    mut builder: redis::cluster::ClusterClientBuilder,
    tls: &RedisTlsOptions,
) -> Result<redis::cluster::ClusterClientBuilder> {
    if !tls.enabled {
        return Ok(builder);
    }

    builder = builder.tls(tls_mode(tls));
    if let Some(certificates) = load_tls_certificates(tls, "cluster").await? {
        builder = builder.certs(certificates);
    }
    Ok(builder)
}

/// Wraps `ClusterClient::get_async_connection` with exponential-backoff retry.
/// The cluster client does not retry initial node discovery on its own.
pub async fn cluster_connect_with_retry(
    client: &redis::cluster::ClusterClient,
) -> Result<redis::cluster_async::ClusterConnection> {
    let mut delay_ms = CLUSTER_CONNECT_BASE_DELAY_MS;
    let mut last_err = None;

    for attempt in 1..=CONNECT_MAX_RETRIES {
        match client.get_async_connection().await {
            Ok(conn) => {
                if attempt > 1 {
                    info!(attempt, "redis cluster connection established after retry");
                }
                return Ok(conn);
            }
            Err(e) => {
                warn!(
                    attempt,
                    max_retries = CONNECT_MAX_RETRIES,
                    retry_in_ms = delay_ms,
                    error = %e,
                    "redis cluster initial connection failed, retrying"
                );
                last_err = Some(e);
                let jitter = (delay_ms / 4) as i64;
                let jittered = (delay_ms as i64 + rand::random_range(-jitter..=jitter)).max(50);
                tokio::time::sleep(Duration::from_millis(jittered as u64)).await;
                delay_ms = delay_ms
                    .saturating_mul(2)
                    .min(CONNECT_MAX_DELAY.as_millis() as u64);
            }
        }
    }

    Err(Error::Redis(format!(
        "redis cluster connection failed after {} attempts: {}",
        CONNECT_MAX_RETRIES,
        last_err.expect("at least one attempt was made")
    )))
}

async fn build_sentinel_client(spec: &SentinelSpec) -> Result<SentinelClient> {
    if spec.hosts.is_empty() {
        return Err(Error::Redis(
            "Redis Sentinel configured without any hosts".to_string(),
        ));
    }

    let addresses: Vec<ConnectionAddr> = spec
        .hosts
        .iter()
        .map(|(host, port)| {
            if spec.sentinel_tls.enabled {
                ConnectionAddr::TcpTls {
                    host: host.clone(),
                    port: *port,
                    insecure: spec.sentinel_tls.accept_invalid_certs,
                    tls_params: None,
                }
            } else {
                ConnectionAddr::Tcp(host.clone(), *port)
            }
        })
        .collect();

    let mut builder = SentinelClientBuilder::new(
        addresses,
        spec.master_name.clone(),
        SentinelServerType::Master,
    )
    .map_err(|error| Error::Redis(format!("failed to initialize Redis Sentinel: {error}")))?;

    if let Some(username) = &spec.sentinel_username {
        builder = builder.set_client_to_sentinel_username(username);
    }
    if let Some(password) = &spec.sentinel_password {
        builder = builder.set_client_to_sentinel_password(password);
    }
    if spec.sentinel_tls.enabled
        && let Some(certificates) = load_tls_certificates(&spec.sentinel_tls, "sentinel").await?
    {
        builder = builder.set_client_to_sentinel_certificates(certificates);
    }

    builder = builder.set_client_to_redis_db(spec.db);
    if let Some(username) = &spec.redis_username {
        builder = builder.set_client_to_redis_username(username);
    }
    if let Some(password) = &spec.redis_password {
        builder = builder.set_client_to_redis_password(password);
    }
    if spec.master_tls.enabled {
        builder = builder.set_client_to_redis_tls_mode(tls_mode(&spec.master_tls));
        if let Some(certificates) = load_tls_certificates(&spec.master_tls, "master").await? {
            builder = builder.set_client_to_redis_certificates(certificates);
        }
    }

    builder
        .build()
        .map_err(|error| Error::Redis(format!("failed to build Redis Sentinel client: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> SentinelSpec {
        SentinelSpec {
            hosts: vec![("127.0.0.1".to_string(), 26379)],
            master_name: "mymaster".to_string(),
            db: 0,
            redis_username: None,
            redis_password: None,
            sentinel_username: None,
            sentinel_password: None,
            master_tls: RedisTlsOptions::default(),
            sentinel_tls: RedisTlsOptions::default(),
        }
    }

    #[tokio::test]
    async fn sentinel_client_build_is_offline_safe() {
        build_sentinel_client(&spec()).await.unwrap();
    }

    #[tokio::test]
    async fn sentinel_requires_hosts() {
        let mut value = spec();
        value.hosts.clear();
        assert!(build_sentinel_client(&value).await.is_err());
    }

    #[tokio::test]
    async fn standalone_tls_upgrades_plain_redis_url() {
        let client = build_standalone_client(
            "redis://127.0.0.1:6379/0",
            &RedisTlsOptions {
                enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("TLS client metadata should build without connecting");

        assert!(matches!(
            client.get_connection_info().addr(),
            ConnectionAddr::TcpTls {
                host,
                port: 6379,
                insecure: false,
                ..
            } if host == "127.0.0.1"
        ));
    }

    #[tokio::test]
    async fn standalone_tls_applies_insecure_mode_explicitly() {
        let client = build_standalone_client(
            "rediss://redis.internal:6380/0",
            &RedisTlsOptions {
                enabled: true,
                accept_invalid_certs: true,
                ..Default::default()
            },
        )
        .await
        .expect("insecure TLS client metadata should build without connecting");

        assert!(matches!(
            client.get_connection_info().addr(),
            ConnectionAddr::TcpTls {
                host,
                port: 6380,
                insecure: true,
                ..
            } if host == "redis.internal"
        ));
    }

    #[tokio::test]
    async fn standalone_tls_reports_missing_private_ca() {
        let result = build_standalone_client(
            "redis://127.0.0.1:6379/0",
            &RedisTlsOptions {
                enabled: true,
                ca_path: Some("/definitely/missing/sockudo-redis-ca.pem".to_string()),
                ..Default::default()
            },
        )
        .await;

        assert!(matches!(result, Err(Error::Redis(message)) if message.contains("TLS CA")));
    }
}
