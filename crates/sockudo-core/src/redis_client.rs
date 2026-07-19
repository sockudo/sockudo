//! Shared standalone and Sentinel-aware Redis connection provider.
//!
//! Command connections are cached and cheap to clone. Worker/listener paths can
//! request a fresh connection so a blocking command never stalls unrelated work.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use redis::aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection, PubSub};
use redis::sentinel::{SentinelClient, SentinelClientBuilder, SentinelServerType};
use redis::{ClientTlsConfig, ConnectionAddr, TlsCertificates, TlsMode};
use tracing::warn;

use crate::error::{Error, Result};
use crate::options::{RedisTlsOptions, SentinelSpec};

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

impl RedisClient {
    /// Connects and eagerly verifies the command path.
    pub async fn connect(url: &str, sentinel: Option<SentinelSpec>) -> Result<Self> {
        let manager_config = ConnectionManagerConfig::new()
            .set_number_of_retries(5)
            .set_exponent_base(2.0)
            .set_max_delay(Duration::from_millis(5_000));

        let source = match sentinel {
            Some(spec) => {
                ClientSource::Sentinel(tokio::sync::Mutex::new(build_sentinel_client(&spec).await?))
            }
            None => ClientSource::Standalone(redis::Client::open(url).map_err(|error| {
                Error::Redis(format!("failed to create Redis client: {error}"))
            })?),
        };

        let client = Self {
            inner: Arc::new(Inner {
                source,
                manager_config,
                connection: Mutex::new(None),
                events_connection: Mutex::new(None),
            }),
        };
        let _ = client.command_connection().await?;
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

    async fn build_manager(&self) -> Result<ConnectionManager> {
        self.master_client()
            .await?
            .get_connection_manager_with_config(self.inner.manager_config.clone())
            .await
            .map_err(|error| Error::Redis(format!("failed to connect to Redis: {error}")))
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

fn tls_mode(tls: &RedisTlsOptions) -> TlsMode {
    if tls.accept_invalid_certs {
        TlsMode::Insecure
    } else {
        TlsMode::Secure
    }
}

async fn load_tls_certificates(
    tls: &RedisTlsOptions,
    hop: &str,
) -> Result<Option<TlsCertificates>> {
    if (tls.client_cert_path.is_some()) ^ (tls.client_key_path.is_some()) {
        warn!(
            "Redis {hop} TLS requires both client_cert_path and client_key_path; ignoring the partial client certificate configuration"
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
}
