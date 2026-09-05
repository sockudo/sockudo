#![cfg(feature = "redis")]

use redis::AsyncCommands;
use sockudo_rate_limiter::RateLimiter;
use sockudo_rate_limiter::redis_limiter::RedisRateLimiter;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn unique_prefix(test_name: &str) -> String {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis();

    format!("sockudo-rate-limiter-live:{test_name}:{now_ms}")
}

#[tokio::test]
#[ignore = "requires a live Redis-compatible server, such as Valkey, on REDIS_URL or localhost:6379"]
async fn redis_limiter_counts_each_request_in_same_second() {
    let client = redis::Client::open(redis_url()).expect("valid Redis URL");
    let prefix = unique_prefix("burst");
    let redis_key = format!("{prefix}:rl:ip-1");
    let limiter = RedisRateLimiter::new(client.clone(), prefix, 3, 60)
        .await
        .expect("Redis limiter should connect");

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("direct Redis connection should connect");
    let _: usize = conn.del(&redis_key).await.expect("test key cleanup");

    let first = limiter.increment("ip-1").await.expect("first increment");
    let second = limiter.increment("ip-1").await.expect("second increment");
    let third = limiter.increment("ip-1").await.expect("third increment");
    let fourth = limiter.increment("ip-1").await.expect("fourth increment");

    let count: usize = conn
        .zcard(&redis_key)
        .await
        .expect("read limiter zset size");
    let _: usize = conn.del(&redis_key).await.expect("test key cleanup");

    assert!(first.allowed);
    assert!(second.allowed);
    assert!(third.allowed);
    assert!(!fourth.allowed);
    assert_eq!(third.remaining, 0);
    assert_eq!(fourth.remaining, 0);
    assert_eq!(count, 3);
}

#[tokio::test]
#[ignore = "requires a live Redis-compatible server, such as Valkey, on REDIS_URL or localhost:6379"]
async fn redis_limiter_removes_expired_window_entries() {
    let client = redis::Client::open(redis_url()).expect("valid Redis URL");
    let prefix = unique_prefix("cleanup");
    let redis_key = format!("{prefix}:rl:ip-1");
    let limiter = RedisRateLimiter::new(client.clone(), prefix, 5, 1)
        .await
        .expect("Redis limiter should connect");

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("direct Redis connection should connect");
    let _: usize = conn.del(&redis_key).await.expect("test key cleanup");
    let _: usize = conn
        .zadd(&redis_key, "expired-member", 0_u64)
        .await
        .expect("insert expired limiter member");

    let before: usize = conn.zcard(&redis_key).await.expect("read seeded zset size");
    let result = limiter.check("ip-1").await.expect("check cleans window");
    let after: usize = conn
        .zcard(&redis_key)
        .await
        .expect("read cleaned zset size");
    let _: usize = conn.del(&redis_key).await.expect("test key cleanup");

    assert_eq!(before, 1);
    assert!(result.allowed);
    assert_eq!(result.remaining, 5);
    assert_eq!(after, 0);
}

#[tokio::test]
#[ignore = "requires a live Redis-compatible server, such as Valkey, on REDIS_URL or localhost:6379"]
async fn redis_limiter_admits_at_most_the_limit_concurrently() {
    let client = redis::Client::open(redis_url()).expect("valid Redis URL");
    let prefix = unique_prefix("atomic-concurrency");
    let redis_key = format!("{prefix}:rl:ip-1");
    let limiter = Arc::new(
        RedisRateLimiter::new(client.clone(), prefix, 10, 60)
            .await
            .expect("Redis limiter should connect"),
    );

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("direct Redis connection should connect");
    let _: usize = conn.del(&redis_key).await.expect("test key cleanup");

    let mut tasks = Vec::with_capacity(100);
    for _ in 0..100 {
        let limiter = Arc::clone(&limiter);
        tasks.push(tokio::spawn(async move {
            limiter.increment("ip-1").await.expect("increment")
        }));
    }

    let mut allowed_count = 0;
    for task in tasks {
        if task.await.expect("increment task").allowed {
            allowed_count += 1;
        }
    }

    let count: usize = conn
        .zcard(&redis_key)
        .await
        .expect("read limiter zset size");
    let _: usize = conn.del(&redis_key).await.expect("test key cleanup");

    assert_eq!(allowed_count, 10);
    assert_eq!(count, 10);
}

#[tokio::test]
#[ignore = "requires a live plaintext Redis-compatible server on REDIS_URL or localhost:6379"]
async fn redis_limiter_retries_after_proxy_drops_idle_connections() {
    let direct_client = redis::Client::open(redis_url()).expect("valid Redis URL");
    let direct_info = direct_client.get_connection_info().clone();
    let target = match direct_info.addr() {
        redis::ConnectionAddr::Tcp(host, port) => format!("{host}:{port}"),
        other => panic!("this live test requires plaintext TCP Redis, got {other:?}"),
    };

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test proxy");
    let proxy_addr = listener.local_addr().expect("proxy address");
    let (drop_connections, _) = broadcast::channel::<()>(1);
    let proxy_drop_connections = drop_connections.clone();
    let proxy_task = tokio::spawn(async move {
        loop {
            let (mut incoming, _) = listener
                .accept()
                .await
                .expect("accept proxied Redis client");
            let mut upstream = TcpStream::connect(&target)
                .await
                .expect("connect proxy to Redis");
            let mut drop_connection = proxy_drop_connections.subscribe();
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut incoming, &mut upstream) => {}
                    _ = drop_connection.recv() => {}
                }
            });
        }
    });

    let proxy_info = direct_info.set_addr(redis::ConnectionAddr::Tcp(
        proxy_addr.ip().to_string(),
        proxy_addr.port(),
    ));
    let proxy_client = redis::Client::open(proxy_info).expect("proxied Redis client");
    let prefix = unique_prefix("idle-drop-retry");
    let redis_key = format!("{prefix}:rl:ip-1");
    let limiter = RedisRateLimiter::new(proxy_client, prefix, 3, 60)
        .await
        .expect("Redis limiter should connect through proxy");

    let mut direct_connection = direct_client
        .get_multiplexed_async_connection()
        .await
        .expect("direct Redis connection should connect");
    let _: usize = direct_connection
        .del(&redis_key)
        .await
        .expect("test key cleanup");

    drop_connections
        .send(())
        .expect("limiter should have active proxied connections");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = limiter
        .increment("ip-1")
        .await
        .expect("limiter should reconnect and retry once");
    let count: usize = direct_connection
        .zcard(&redis_key)
        .await
        .expect("read limiter zset size");
    let _: usize = direct_connection
        .del(&redis_key)
        .await
        .expect("test key cleanup");
    proxy_task.abort();

    assert!(result.allowed);
    assert_eq!(result.remaining, 2);
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires a live plaintext Redis-compatible server on REDIS_URL or localhost:6379"]
async fn redis_limiter_coalesces_a_concurrent_reconnect_storm() {
    let direct_client = redis::Client::open(redis_url()).expect("valid Redis URL");
    let direct_info = direct_client.get_connection_info().clone();
    let target = match direct_info.addr() {
        redis::ConnectionAddr::Tcp(host, port) => format!("{host}:{port}"),
        other => panic!("this live test requires plaintext TCP Redis, got {other:?}"),
    };

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test proxy");
    let proxy_addr = listener.local_addr().expect("proxy address");
    let (drop_connections, _) = broadcast::channel::<()>(1);
    let proxy_drop_connections = drop_connections.clone();
    let accepted_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let proxy_accepted = accepted_connections.clone();
    let proxy_task = tokio::spawn(async move {
        loop {
            let (mut incoming, _) = listener
                .accept()
                .await
                .expect("accept proxied Redis client");
            proxy_accepted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut upstream = TcpStream::connect(&target)
                .await
                .expect("connect proxy to Redis");
            let mut drop_connection = proxy_drop_connections.subscribe();
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut incoming, &mut upstream) => {}
                    _ = drop_connection.recv() => {}
                }
            });
        }
    });

    let proxy_info = direct_info.set_addr(redis::ConnectionAddr::Tcp(
        proxy_addr.ip().to_string(),
        proxy_addr.port(),
    ));
    let proxy_client = redis::Client::open(proxy_info).expect("proxied Redis client");
    let prefix = unique_prefix("idle-drop-retry");
    let redis_key = format!("{prefix}:rl:ip-1");
    let limiter = RedisRateLimiter::new(proxy_client, prefix, 128, 60)
        .await
        .expect("Redis limiter should connect through proxy");

    let mut direct_connection = direct_client
        .get_multiplexed_async_connection()
        .await
        .expect("direct Redis connection should connect");
    let _: usize = direct_connection
        .del(&redis_key)
        .await
        .expect("test key cleanup");

    let limiter = Arc::new(limiter);
    let before = accepted_connections.load(std::sync::atomic::Ordering::SeqCst);
    drop_connections
        .send(())
        .expect("limiter should have active proxied connections");

    let mut tasks = Vec::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(128));
    for _ in 0..128 {
        let limiter = limiter.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            limiter.increment("ip-1").await.unwrap()
        }));
    }
    for task in tasks {
        assert!(task.await.unwrap().allowed);
    }
    let count: usize = direct_connection
        .zcard(&redis_key)
        .await
        .expect("read limiter zset size");
    let _: usize = direct_connection
        .del(&redis_key)
        .await
        .expect("test key cleanup");
    proxy_task.abort();

    assert_eq!(count, 128);
    let reconnects = accepted_connections.load(std::sync::atomic::Ordering::SeqCst) - before;
    assert!(
        reconnects <= 4,
        "reconnection must be shared, observed {reconnects} new TCP connections"
    );
}
