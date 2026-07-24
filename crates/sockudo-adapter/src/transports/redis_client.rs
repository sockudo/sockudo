//! Sentinel-aware Redis client used by the horizontal [`RedisTransport`].
//!
//! Standalone deployments build a plain [`redis::Client`] from a URL exactly as
//! before. Sentinel deployments build a native [`redis::sentinel::SentinelClient`]
//! (the `redis+sentinel://` URL scheme is rejected by `redis::Client::open`),
//! applying TLS and optional client-certificate (mutual TLS) settings independently
//! to the client→sentinel control hop and the client→master data hop.
//!
//! [`RedisClient`] presents a single interface over both modes:
//! - cached command [`redis::aio::ConnectionManager`]s for publishing,
//! - freshly-resolved pub/sub and health connections.
//!
//! For Sentinel, [`RedisClient::invalidate`] drops the cached command connections
//! so the next call re-resolves the current master, which is how master failover is
//! handled on the command path. The pub/sub listener re-resolves the master on every
//! (re)connect, so failover is handled there by the existing reconnect loop.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use redis::aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection, PubSub};
use redis::sentinel::{SentinelClient, SentinelClientBuilder, SentinelServerType};
use redis::{ClientTlsConfig, ConnectionAddr, TlsCertificates, TlsMode};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{RedisTlsOptions, SentinelSpec};
use tracing::warn;

/// Backing connection source: a plain client, or a Sentinel client that resolves
/// the current master on demand.
enum ClientSource {
    Standalone(redis::Client),
    /// `SentinelClient` resolution methods take `&mut self`, so it is guarded by an
    /// async mutex. The guard is intentionally held across `.await` while resolving.
    Sentinel(tokio::sync::Mutex<SentinelClient>),
}

struct Inner {
    source: ClientSource,
    cm_config: ConnectionManagerConfig,
    /// Cached command connection (request/response/node publishing, PUBSUB queries).
    connection: Mutex<Option<ConnectionManager>>,
    /// Cached connection dedicated to events-API broadcasts.
    events_connection: Mutex<Option<ConnectionManager>>,
}

/// Cheap-to-clone handle over a standalone or Sentinel-managed Redis deployment.
#[derive(Clone)]
pub struct RedisClient {
    inner: Arc<Inner>,
}

impl RedisClient {
    /// Connects to Redis.
    ///
    /// When `sentinel` is `Some`, a native Sentinel client is built and `url` is
    /// ignored. Otherwise `url` is opened as a standalone client (supporting
    /// `redis://` and `rediss://`).
    ///
    /// Command connections are established eagerly so that an unreachable Redis
    /// fails fast at construction, matching prior behavior.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Redis`] if the client cannot be built or an initial
    /// connection cannot be established.
    pub async fn connect(url: &str, sentinel: Option<SentinelSpec>) -> Result<Self> {
        let cm_config = ConnectionManagerConfig::new()
            .set_number_of_retries(5)
            .set_exponent_base(2.0)
            .set_max_delay(Duration::from_millis(5000));

        let source = match sentinel {
            Some(spec) => {
                ClientSource::Sentinel(tokio::sync::Mutex::new(build_sentinel_client(&spec).await?))
            }
            None => {
                let client = redis::Client::open(url)
                    .map_err(|e| Error::Redis(format!("Failed to create Redis client: {e}")))?;
                ClientSource::Standalone(client)
            }
        };

        let client = Self {
            inner: Arc::new(Inner {
                source,
                cm_config,
                connection: Mutex::new(None),
                events_connection: Mutex::new(None),
            }),
        };

        // Establish both command connections up front (fail fast).
        let _ = client.command_connection().await?;
        let _ = client.events_connection().await?;

        Ok(client)
    }

    /// Resolves a [`redis::Client`] pointed at the current master.
    ///
    /// For Sentinel this queries the sentinels for the current master, providing
    /// failover handling: callers that rebuild connections after an error reconnect
    /// to the new master.
    async fn master_client(&self) -> Result<redis::Client> {
        match &self.inner.source {
            ClientSource::Standalone(client) => Ok(client.clone()),
            ClientSource::Sentinel(sentinel) => {
                let mut guard = sentinel.lock().await;
                guard
                    .async_get_client()
                    .await
                    .map_err(|e| Error::Redis(format!("Failed to resolve Sentinel master: {e}")))
            }
        }
    }

    async fn get_or_build(
        &self,
        slot: &Mutex<Option<ConnectionManager>>,
    ) -> Result<ConnectionManager> {
        // Fast path: return the cached manager without holding the lock across await.
        {
            let guard = slot.lock();
            if let Some(manager) = guard.as_ref() {
                return Ok(manager.clone());
            }
        }

        let client = self.master_client().await?;
        let manager = client
            .get_connection_manager_with_config(self.inner.cm_config.clone())
            .await
            .map_err(|e| Error::Redis(format!("Failed to connect to Redis: {e}")))?;

        *slot.lock() = Some(manager.clone());
        Ok(manager)
    }

    /// Returns the command connection used for request/response/node publishing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Redis`] if a connection cannot be (re)established.
    pub async fn command_connection(&self) -> Result<ConnectionManager> {
        self.get_or_build(&self.inner.connection).await
    }

    /// Returns the connection dedicated to events-API broadcasts.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Redis`] if a connection cannot be (re)established.
    pub async fn events_connection(&self) -> Result<ConnectionManager> {
        self.get_or_build(&self.inner.events_connection).await
    }

    /// Drops cached command connections so the next call re-resolves the master.
    ///
    /// This is the master-failover hook for the command path. It is a no-op for
    /// standalone clients, whose [`ConnectionManager`] reconnects on its own.
    pub fn invalidate(&self) {
        if matches!(self.inner.source, ClientSource::Sentinel(_)) {
            *self.inner.connection.lock() = None;
            *self.inner.events_connection.lock() = None;
        }
    }

    /// Establishes a fresh pub/sub connection to the current master.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Redis`] if the master cannot be resolved or the pub/sub
    /// connection cannot be established.
    pub async fn pubsub(&self) -> Result<PubSub> {
        let client = self.master_client().await?;
        client
            .get_async_pubsub()
            .await
            .map_err(|e| Error::Redis(format!("Failed to get pubsub connection: {e}")))
    }

    /// Establishes a fresh multiplexed connection to the current master (health checks).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Redis`] if the master cannot be resolved or the connection
    /// cannot be established.
    pub async fn multiplexed(&self) -> Result<MultiplexedConnection> {
        let client = self.master_client().await?;
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Redis(format!("Failed to acquire connection: {e}")))
    }
}

fn tls_mode(tls: &RedisTlsOptions) -> TlsMode {
    if tls.accept_invalid_certs {
        TlsMode::Insecure
    } else {
        TlsMode::Secure
    }
}

/// Loads PEM CA and/or client certificate material referenced by `tls`.
///
/// Returns `None` when neither a CA nor a complete client cert/key pair is
/// configured, in which case the system/webpki root store is used and mutual TLS
/// is disabled.
async fn load_tls_certificates(
    tls: &RedisTlsOptions,
    hop: &str,
) -> Result<Option<TlsCertificates>> {
    if (tls.client_cert_path.is_some()) ^ (tls.client_key_path.is_some()) {
        warn!(
            adapter = "redis",
            hop = %hop,
            "partial mutual tls configuration ignored"
        );
    }

    if tls.ca_path.is_none() && !tls.has_client_cert() {
        return Ok(None);
    }

    let root_cert = match &tls.ca_path {
        Some(path) => Some(tokio::fs::read(path).await.map_err(|e| {
            Error::Redis(format!(
                "Failed to read Redis {hop} TLS CA certificate {path}: {e}"
            ))
        })?),
        None => None,
    };

    let client_tls = match (&tls.client_cert_path, &tls.client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let client_cert = tokio::fs::read(cert_path).await.map_err(|e| {
                Error::Redis(format!(
                    "Failed to read Redis {hop} TLS client certificate {cert_path}: {e}"
                ))
            })?;
            let client_key = tokio::fs::read(key_path).await.map_err(|e| {
                Error::Redis(format!(
                    "Failed to read Redis {hop} TLS client key {key_path}: {e}"
                ))
            })?;
            Some(ClientTlsConfig {
                client_cert,
                client_key,
            })
        }
        _ => None,
    };

    Ok(Some(TlsCertificates {
        client_tls,
        root_cert,
    }))
}

/// Builds a [`SentinelClient`] from a [`SentinelSpec`], applying auth and TLS to
/// both the client→sentinel and client→master hops.
async fn build_sentinel_client(spec: &SentinelSpec) -> Result<SentinelClient> {
    if spec.hosts.is_empty() {
        return Err(Error::Redis(
            "Sentinel configured but no sentinel hosts were provided".to_string(),
        ));
    }

    // TLS for the sentinel control hop is expressed by the address variant itself:
    // `redis-rs` rejects `set_client_to_sentinel_tls_mode`/`_certificates` unless the
    // sentinel addresses are `TcpTls`. The master/replica hop is configured separately
    // via the `set_client_to_redis_*` setters below.
    let addrs: Vec<ConnectionAddr> = spec
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

    let mut builder =
        SentinelClientBuilder::new(addrs, spec.master_name.clone(), SentinelServerType::Master)
            .map_err(|e| {
                Error::Redis(format!("Failed to initialize Sentinel client builder: {e}"))
            })?;

    // Client -> Sentinel control hop. The TLS mode is already encoded in the address
    // variant above; only auth and (optional) client certificates are applied here.
    if let Some(username) = &spec.sentinel_username {
        builder = builder.set_client_to_sentinel_username(username);
    }
    if let Some(password) = &spec.sentinel_password {
        builder = builder.set_client_to_sentinel_password(password);
    }
    if spec.sentinel_tls.enabled
        && let Some(certs) = load_tls_certificates(&spec.sentinel_tls, "sentinel").await?
    {
        builder = builder.set_client_to_sentinel_certificates(certs);
    }

    // Client -> master/replica data hop.
    builder = builder.set_client_to_redis_db(spec.db);
    if let Some(username) = &spec.redis_username {
        builder = builder.set_client_to_redis_username(username);
    }
    if let Some(password) = &spec.redis_password {
        builder = builder.set_client_to_redis_password(password);
    }
    if spec.master_tls.enabled {
        builder = builder.set_client_to_redis_tls_mode(tls_mode(&spec.master_tls));
        if let Some(certs) = load_tls_certificates(&spec.master_tls, "master").await? {
            builder = builder.set_client_to_redis_certificates(certs);
        }
    }

    builder
        .build()
        .map_err(|e| Error::Redis(format!("Failed to build Sentinel client: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("sockudo-redis-tls-{}-{name}", uuid::Uuid::new_v4()));
        path
    }

    fn base_spec() -> SentinelSpec {
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
    async fn load_tls_certificates_returns_none_when_unconfigured() {
        let tls = RedisTlsOptions {
            enabled: true,
            ..Default::default()
        };
        let result = load_tls_certificates(&tls, "master").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn load_tls_certificates_reads_ca_and_client_pair() {
        let ca = temp_path("ca.pem");
        let cert = temp_path("client.pem");
        let key = temp_path("client.key");
        tokio::fs::write(&ca, b"-----CA-----").await.unwrap();
        tokio::fs::write(&cert, b"-----CERT-----").await.unwrap();
        tokio::fs::write(&key, b"-----KEY-----").await.unwrap();

        let tls = RedisTlsOptions {
            enabled: true,
            ca_path: Some(ca.to_string_lossy().into_owned()),
            client_cert_path: Some(cert.to_string_lossy().into_owned()),
            client_key_path: Some(key.to_string_lossy().into_owned()),
            ..Default::default()
        };

        let certs = load_tls_certificates(&tls, "master")
            .await
            .unwrap()
            .expect("certificates should be loaded");
        assert_eq!(certs.root_cert.as_deref(), Some(&b"-----CA-----"[..]));
        let client = certs.client_tls.expect("client tls should be present");
        assert_eq!(client.client_cert, b"-----CERT-----");
        assert_eq!(client.client_key, b"-----KEY-----");

        let _ = tokio::fs::remove_file(&ca).await;
        let _ = tokio::fs::remove_file(&cert).await;
        let _ = tokio::fs::remove_file(&key).await;
    }

    #[tokio::test]
    async fn load_tls_certificates_errors_on_missing_file() {
        let tls = RedisTlsOptions {
            enabled: true,
            ca_path: Some(temp_path("missing-ca.pem").to_string_lossy().into_owned()),
            ..Default::default()
        };
        assert!(load_tls_certificates(&tls, "master").await.is_err());
    }

    #[tokio::test]
    async fn build_sentinel_client_succeeds_plaintext() {
        // Building does not open a network connection, so this is offline-safe.
        build_sentinel_client(&base_spec())
            .await
            .expect("plaintext sentinel client should build");
    }

    #[tokio::test]
    async fn build_sentinel_client_succeeds_with_tls_no_certs() {
        let mut spec = base_spec();
        spec.sentinel_tls = RedisTlsOptions {
            enabled: true,
            accept_invalid_certs: true,
            ..Default::default()
        };
        spec.master_tls = RedisTlsOptions {
            enabled: true,
            ..Default::default()
        };
        spec.redis_password = Some("secret".to_string());
        spec.sentinel_password = Some("sentinel-secret".to_string());
        build_sentinel_client(&spec)
            .await
            .expect("TLS sentinel client should build");
    }

    #[tokio::test]
    async fn build_sentinel_client_errors_without_hosts() {
        let mut spec = base_spec();
        spec.hosts.clear();
        assert!(build_sentinel_client(&spec).await.is_err());
    }
}
