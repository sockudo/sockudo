//! Live Redis Sentinel integration tests.
//!
//! These are `#[ignore]`d and excluded from the default `cargo test` run, matching
//! the convention in `sockudo-rate-limiter/tests/redis_limiter_live.rs`. They exercise
//! the real Sentinel connection path — master resolution, TLS, and mutual TLS — which
//! the offline unit tests in `transports::redis_client` cannot cover.
//!
//! Run against the bundled local fixture:
//!
//! ```bash
//! make sentinel-tls-up
//! # Cert paths must be absolute: `cargo test -p` sets the test binary's working
//! # directory to the crate dir, not the workspace root.
//! SOCKUDO_SENTINEL_TLS=1 SOCKUDO_MASTER_TLS=1 \
//!   SOCKUDO_REDIS_PASSWORD=masterpass \
//!   SOCKUDO_TLS_CA_PATH="$PWD/tests/sentinel-tls/certs/ca.crt" \
//!   SOCKUDO_TLS_CLIENT_CERT_PATH="$PWD/tests/sentinel-tls/certs/client.crt" \
//!   SOCKUDO_TLS_CLIENT_KEY_PATH="$PWD/tests/sentinel-tls/certs/client.key" \
//!   cargo test -p sockudo-adapter --features redis --test redis_sentinel_live -- --ignored
//! ```
//!
//! Or point the same env knobs at a real deployment (e.g. a staging Sentinel cluster)
//! to validate production credentials and certificates end to end.

#![cfg(feature = "redis")]

use std::time::{SystemTime, UNIX_EPOCH};

use sockudo_adapter::horizontal_transport::HorizontalTransport;
use sockudo_adapter::transports::{RedisAdapterConfig, RedisTransport};
use sockudo_core::options::{RedisConnection, RedisSentinel, RedisTlsOptions};

const IGNORE_REASON: &str = "requires a live Redis Sentinel deployment; configure via SOCKUDO_SENTINEL_* env vars (see `make sentinel-tls-up`)";

fn env_bool(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
}

fn env_opt(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn sentinel_hosts() -> Vec<RedisSentinel> {
    let raw = std::env::var("SOCKUDO_SENTINEL_HOSTS")
        .unwrap_or_else(|_| "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381".to_string());

    raw.split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            let (host, port) = entry.rsplit_once(':')?;
            Some(RedisSentinel {
                host: host.to_string(),
                port: port.parse().ok()?,
            })
        })
        .collect()
}

fn tls_from_env(enabled_var: &str) -> RedisTlsOptions {
    RedisTlsOptions {
        enabled: env_bool(enabled_var),
        accept_invalid_certs: env_bool("SOCKUDO_TLS_INSECURE"),
        ca_path: env_opt("SOCKUDO_TLS_CA_PATH"),
        client_cert_path: env_opt("SOCKUDO_TLS_CLIENT_CERT_PATH"),
        client_key_path: env_opt("SOCKUDO_TLS_CLIENT_KEY_PATH"),
    }
}

fn sentinel_connection() -> RedisConnection {
    RedisConnection {
        db: env_opt("SOCKUDO_REDIS_DB")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        username: env_opt("SOCKUDO_REDIS_USERNAME"),
        password: env_opt("SOCKUDO_REDIS_PASSWORD"),
        sentinels: sentinel_hosts(),
        sentinel_password: env_opt("SOCKUDO_SENTINEL_PASSWORD"),
        sentinel_username: env_opt("SOCKUDO_SENTINEL_USERNAME"),
        name: std::env::var("SOCKUDO_SENTINEL_MASTER_NAME")
            .unwrap_or_else(|_| "mymaster".to_string()),
        sentinel_tls: tls_from_env("SOCKUDO_SENTINEL_TLS"),
        master_tls: tls_from_env("SOCKUDO_MASTER_TLS"),
        ..Default::default()
    }
}

fn transport_config(test_name: &str) -> RedisAdapterConfig {
    let connection = sentinel_connection();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    RedisAdapterConfig {
        url: "redis+sentinel://live-test".to_string(),
        prefix: format!("sockudo-sentinel-live:{test_name}:{now_ms}"),
        request_timeout_ms: 5000,
        cluster_mode: false,
        sentinel: connection.sentinel_spec(),
        tls: connection.master_tls,
        pubsub_ping_interval_ms: 10_000,
    }
}

fn direct_transport_config(test_name: &str) -> RedisAdapterConfig {
    let mut connection = sentinel_connection();
    connection.sentinels.clear();
    connection.host =
        std::env::var("SOCKUDO_DIRECT_REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    connection.port = std::env::var("SOCKUDO_DIRECT_REDIS_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16380);
    let url = connection.to_url();

    RedisAdapterConfig {
        url,
        prefix: format!("sockudo-direct-tls-live:{test_name}"),
        request_timeout_ms: 5000,
        cluster_mode: false,
        sentinel: None,
        tls: connection.master_tls,
        pubsub_ping_interval_ms: 10_000,
    }
}

#[tokio::test]
#[ignore = "requires a live direct Redis TLS deployment; configure via SOCKUDO_MASTER_TLS and SOCKUDO_TLS_* (see `make sentinel-tls-up`)"]
async fn direct_transport_connects_with_private_ca() {
    let transport = RedisTransport::new(direct_transport_config("private-ca"))
        .await
        .expect("direct transport should connect with the configured private CA and mTLS pair");

    transport
        .check_health()
        .await
        .expect("PING via the direct TLS connection should succeed");
}

#[tokio::test]
#[ignore = "requires a live Redis Sentinel deployment; configure via SOCKUDO_SENTINEL_* env vars (see `make sentinel-tls-up`)"]
async fn sentinel_transport_connects_and_pings() {
    assert!(
        sentinel_connection().sentinel_spec().is_some(),
        "{IGNORE_REASON}"
    );

    let transport = RedisTransport::new(transport_config("ping"))
        .await
        .expect("sentinel transport should connect (TLS/mTLS + master resolution)");

    transport
        .check_health()
        .await
        .expect("PING via the Sentinel-resolved master should succeed");
}

#[tokio::test]
#[ignore = "requires a live Redis Sentinel deployment; configure via SOCKUDO_SENTINEL_* env vars (see `make sentinel-tls-up`)"]
async fn sentinel_transport_reports_node_count() {
    let transport = RedisTransport::new(transport_config("nodes"))
        .await
        .expect("sentinel transport should connect (TLS/mTLS + master resolution)");

    let node_count = transport
        .get_node_count()
        .await
        .expect("PUBSUB NUMSUB via the Sentinel-resolved master should succeed");

    assert!(
        node_count >= 1,
        "node count should be at least 1, got {node_count}"
    );
}

/// Asks the first reachable Sentinel for the current primary.
async fn sentinel_primary(connection: &RedisConnection) -> Option<(String, u16)> {
    for sentinel in &connection.sentinels {
        let addr = if connection.sentinel_tls.enabled {
            redis::ConnectionAddr::TcpTls {
                host: sentinel.host.clone(),
                port: sentinel.port,
                insecure: true,
                tls_params: None,
            }
        } else {
            redis::ConnectionAddr::Tcp(sentinel.host.clone(), sentinel.port)
        };
        let mut settings = redis::RedisConnectionInfo::default();
        if let Some(password) = &connection.sentinel_password {
            settings = settings.set_password(password);
        }
        let Ok(info) = redis::IntoConnectionInfo::into_connection_info(addr) else {
            continue;
        };
        let Ok(client) = redis::Client::open(info.set_redis_settings(settings)) else {
            continue;
        };
        let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
            continue;
        };
        if let Ok(Some((host, port))) = redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(&connection.name)
            .query_async::<Option<(String, u16)>>(&mut conn)
            .await
        {
            return Some((host, port));
        }
    }
    None
}

/// Triggers a failover: a graceful `SENTINEL FAILOVER` by default, or a hard
/// primary shutdown when `SOCKUDO_SENTINEL_FAILOVER_SHUTDOWN=1`.
async fn trigger_failover(connection: &RedisConnection, primary: &(String, u16)) {
    if env_bool("SOCKUDO_SENTINEL_FAILOVER_SHUTDOWN") {
        let mut settings = redis::RedisConnectionInfo::default();
        if let Some(password) = &connection.password {
            settings = settings.set_password(password);
        }
        let info = redis::IntoConnectionInfo::into_connection_info(redis::ConnectionAddr::Tcp(
            primary.0.clone(),
            primary.1,
        ))
        .expect("primary address")
        .set_redis_settings(settings);
        let mut conn = redis::Client::open(info)
            .expect("primary client")
            .get_multiplexed_async_connection()
            .await
            .expect("connect to the current primary");
        // SHUTDOWN closes the connection instead of replying.
        let _ = redis::cmd("SHUTDOWN")
            .arg("NOSAVE")
            .query_async::<()>(&mut conn)
            .await;
        return;
    }

    let sentinel = &connection.sentinels[0];
    let mut settings = redis::RedisConnectionInfo::default();
    if let Some(password) = &connection.sentinel_password {
        settings = settings.set_password(password);
    }
    let info = redis::IntoConnectionInfo::into_connection_info(redis::ConnectionAddr::Tcp(
        sentinel.host.clone(),
        sentinel.port,
    ))
    .expect("sentinel address")
    .set_redis_settings(settings);
    let mut conn = redis::Client::open(info)
        .expect("sentinel client")
        .get_multiplexed_async_connection()
        .await
        .expect("connect to sentinel");
    redis::cmd("SENTINEL")
        .arg("FAILOVER")
        .arg(&connection.name)
        .query_async::<()>(&mut conn)
        .await
        .expect("SENTINEL FAILOVER should be accepted");
}

/// Regression for Sentinel failover: cached cache/health connections and the
/// adapter's Pub/Sub listener must follow the promoted primary without a
/// restart. Needs a plain-TCP Sentinel fixture whose primary has at least one
/// replica (the bundled TLS fixture has none), e.g. a local `redis-server`
/// primary + replica monitored by three `redis-sentinel` processes:
///
/// ```bash
/// SOCKUDO_SENTINEL_HOSTS=127.0.0.1:26390,127.0.0.1:26391,127.0.0.1:26392 \
///   cargo test -p sockudo-adapter --features redis --test redis_sentinel_live \
///   sentinel_clients_follow_primary_failover -- --ignored --nocapture
/// ```
///
/// Set `SOCKUDO_SENTINEL_FAILOVER_SHUTDOWN=1` to kill the primary instead of
/// asking Sentinel for a graceful failover.
#[tokio::test]
#[ignore = "requires a live Redis Sentinel deployment with a replica; see the test docs"]
async fn sentinel_clients_follow_primary_failover() {
    use sockudo_adapter::horizontal_transport::{BoxFuture, TransportHandlers};
    use sockudo_cache::{RedisCacheConfig, RedisCacheManager};
    use sockudo_core::cache::CacheManager;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    let connection = sentinel_connection();
    let cache = RedisCacheManager::new(RedisCacheConfig {
        url: "redis+sentinel://live-test".to_string(),
        prefix: format!("sockudo-sentinel-failover:{}", std::process::id()),
        response_timeout: Some(Duration::from_secs(1)),
        sentinel: connection.sentinel_spec(),
        tls: connection.master_tls.clone(),
        ..Default::default()
    })
    .await
    .expect("cache should connect through Sentinel");

    let config = transport_config("failover");
    let subscriber = RedisTransport::new(config.clone())
        .await
        .expect("subscriber transport should connect");
    let publisher = RedisTransport::new(config)
        .await
        .expect("publisher transport should connect");

    let (received_tx, mut received) = mpsc::unbounded_channel::<String>();
    subscriber
        .start_listeners(TransportHandlers {
            node_id: "failover-subscriber".to_string(),
            on_broadcast: Arc::new(move |message| {
                let _ = received_tx.send(message.message.clone());
                Box::pin(async {}) as BoxFuture<'static, ()>
            }),
            on_request: Arc::new(|_| {
                Box::pin(async { Err(sockudo_core::error::Error::Other("not used".to_string())) })
            }),
            on_response: Arc::new(|_| Box::pin(async {})),
        })
        .await
        .expect("listeners should start");

    // Delivers a uniquely tagged broadcast, retrying until `deadline`.
    async fn deliver(
        publisher: &RedisTransport,
        received: &mut mpsc::UnboundedReceiver<String>,
        tag: &str,
        deadline: tokio::time::Instant,
    ) -> bool {
        while tokio::time::Instant::now() < deadline {
            let message = sockudo_adapter::horizontal_adapter::BroadcastMessage {
                node_id: "failover-publisher".to_string(),
                app_id: "failover-app".to_string(),
                channel: "failover-channel".to_string(),
                message: tag.to_string(),
                presence_replication: None,
                envelope: None,
                except_socket_id: None,
                timestamp_ms: None,
                compression_metadata: None,
                idempotency_key: None,
                ephemeral: false,
                trace_context: Default::default(),
            };
            let _ = publisher.publish_broadcast(&message).await;
            let wait = tokio::time::sleep(Duration::from_millis(500));
            tokio::pin!(wait);
            loop {
                tokio::select! {
                    _ = &mut wait => break,
                    Some(body) = received.recv() => {
                        if body == tag {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    let settle = tokio::time::Instant::now() + Duration::from_secs(10);
    assert!(
        deliver(&publisher, &mut received, "before-failover", settle).await,
        "broadcast should be delivered before failover"
    );
    cache
        .check_health()
        .await
        .expect("cache healthy before failover");

    let primary = sentinel_primary(&connection)
        .await
        .expect("sentinel should report a primary");
    trigger_failover(&connection, &primary).await;

    let promoted_by = tokio::time::Instant::now() + Duration::from_secs(90);
    let promoted = loop {
        if let Some(current) = sentinel_primary(&connection).await
            && current != primary
        {
            break current;
        }
        assert!(
            tokio::time::Instant::now() < promoted_by,
            "sentinel did not promote a new primary"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    println!("sentinel promoted {promoted:?} (was {primary:?})");

    // Recovery must happen promptly and without rebuilding any client.
    let recovered_by = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let healthy = cache.check_health().await.is_ok()
            && cache.set("failover-probe", "ok", 30).await.is_ok();
        if healthy {
            break;
        }
        assert!(
            tokio::time::Instant::now() < recovered_by,
            "cache did not recover on the promoted primary"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        deliver(&publisher, &mut received, "after-failover", recovered_by).await,
        "broadcast should be delivered through the promoted primary"
    );
}
