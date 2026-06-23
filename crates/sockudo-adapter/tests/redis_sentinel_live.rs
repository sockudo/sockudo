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
        serialization: sockudo_adapter::serialization::Serialization::default(
        ),
    }
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
