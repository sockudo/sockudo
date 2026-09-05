//! Real broker regressions; each run uses an isolated prefix and keeps all accepted records.
#![cfg(any(feature = "kafka", feature = "rabbitmq", feature = "redis"))]
#[allow(dead_code)]
#[path = "adapter/transports/test_helpers.rs"]
mod helpers;
use helpers::*;
use sockudo_adapter::horizontal_transport::HorizontalTransport;
use std::time::Duration;

async fn ordered_two_node_fanout<T: HorizontalTransport>(config: T::Config)
where
    T::Config: Clone,
{
    let first = T::new(config.clone()).await.unwrap();
    let second = T::new(config).await.unwrap();
    let a = MessageCollector::new();
    let b = MessageCollector::new();
    first
        .start_listeners(create_test_handlers(a.clone()))
        .await
        .unwrap();
    second
        .start_listeners(create_test_handlers(b.clone()))
        .await
        .unwrap();
    // Kafka starts from latest after assignment. Prove both live subscriptions
    // before beginning the accepted workload; subscription setup is excluded.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        first
            .publish_broadcast(&create_test_broadcast("ready"))
            .await
            .unwrap();
        if a.wait_for_broadcast(200).await.is_some() && b.wait_for_broadcast(200).await.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "both consumers must become ready"
        );
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    a.clear().await;
    b.clear().await;
    for serial in 0..128 {
        let mut message = create_test_broadcast("ordered");
        message.message = serial.to_string();
        first.publish_broadcast(&message).await.unwrap();
    }
    for collector in [&a, &b] {
        assert!(
            wait_for_condition(
                || async { collector.get_broadcasts().await.len() == 128 },
                10000
            )
            .await
        );
        let received = collector.get_broadcasts().await;
        assert_eq!(
            received
                .iter()
                .map(|message| message.message.parse::<usize>().unwrap())
                .collect::<Vec<_>>(),
            (0..128).collect::<Vec<_>>()
        );
    }
    first.publish_request(&create_test_request()).await.unwrap();
    assert!(a.wait_for_response(5000).await.is_some());
    assert!(b.wait_for_response(5000).await.is_some());
    first.check_health().await.unwrap();
}

#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires SOCKUDO_KAFKA_TEST_BROKER"]
async fn kafka_partitioned_epoch_preserves_fanout_order_and_rejects_repartition() {
    use sockudo_adapter::transports::KafkaTransport;
    let config = sockudo_core::options::KafkaAdapterConfig {
        brokers: vec![std::env::var("SOCKUDO_KAFKA_TEST_BROKER").unwrap()],
        prefix: format!("sockudo-fanout-live-{}", uuid::Uuid::new_v4()),
        topic_epoch: Some("test1".into()),
        partitions: 4,
        ..Default::default()
    };
    ordered_two_node_fanout::<KafkaTransport>(config.clone()).await;
    let bad = sockudo_core::options::KafkaAdapterConfig {
        partitions: 8,
        ..config
    };
    assert!(KafkaTransport::new(bad).await.is_err());
}

#[cfg(feature = "rabbitmq")]
#[tokio::test]
#[ignore = "requires SOCKUDO_RABBITMQ_TEST_URL"]
async fn rabbitmq_confirmed_fanout_preserves_order_and_control_replies() {
    let config = sockudo_core::options::RabbitMqAdapterConfig {
        url: std::env::var("SOCKUDO_RABBITMQ_TEST_URL").unwrap(),
        prefix: format!("sockudo-fanout-live-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };
    ordered_two_node_fanout::<sockudo_adapter::transports::RabbitMqTransport>(config).await;
}

#[cfg(feature = "redis-cluster")]
fn cluster_config(sharded: bool) -> sockudo_core::options::RedisClusterAdapterConfig {
    sockudo_core::options::RedisClusterAdapterConfig {
        nodes: std::env::var("SOCKUDO_REDIS_CLUSTER_TEST_NODES")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect(),
        prefix: format!("sockudo-fanout-live-{}", uuid::Uuid::new_v4()),
        use_sharded_pubsub: sharded,
        ..Default::default()
    }
}

#[cfg(feature = "redis-cluster")]
#[tokio::test]
#[ignore = "requires SOCKUDO_REDIS_CLUSTER_TEST_NODES"]
async fn redis_cluster_standard_and_sharded_ordered_delivery() {
    for sharded in [false, true] {
        ordered_two_node_fanout::<sockudo_adapter::transports::RedisClusterTransport>(
            cluster_config(sharded),
        )
        .await;
    }
}

#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires audit-only authenticated Sentinel fixture at 26395 and data ports 16395/16396"]
async fn redis_sentinel_resp3_reconnect_preserves_auth_and_invalidates_gap() {
    use sockudo_adapter::transports::{RedisAdapterConfig, RedisTransport};
    use sockudo_core::options::SentinelSpec;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    assert_eq!(
        std::env::var("SOCKUDO_SENTINEL_FAILOVER_TEST").as_deref(),
        Ok("1")
    );
    let mut sentinel = redis::Client::open("redis://:fixture-sentinel@127.0.0.1:26395/")
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let initial: Vec<String> = redis::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg("fanout")
        .query_async(&mut sentinel)
        .await
        .unwrap();
    let old_port: u16 = initial[1].parse().unwrap();
    assert!([16395, 16396].contains(&old_port));
    let config = RedisAdapterConfig {
        prefix: format!("sentinel-live-{}", uuid::Uuid::new_v4()),
        sentinel: Some(SentinelSpec {
            hosts: vec![("127.0.0.1".into(), 26395)],
            master_name: "fanout".into(),
            db: 7,
            redis_username: None,
            redis_password: Some("fixture-data".into()),
            sentinel_username: None,
            sentinel_password: Some("fixture-sentinel".into()),
            master_tls: Default::default(),
            sentinel_tls: Default::default(),
        }),
        ..Default::default()
    };
    let receiver = RedisTransport::new(config.clone()).await.unwrap();
    let collector = MessageCollector::new();
    let gaps = Arc::new(AtomicUsize::new(0));
    let observed_gaps = gaps.clone();
    let mut handlers = create_test_handlers(collector.clone());
    handlers.on_ingress_gap = Arc::new(move |_, _| {
        observed_gaps.fetch_add(1, Ordering::SeqCst);
    });
    receiver.start_listeners(handlers).await.unwrap();
    let wait_ready = |publisher: RedisTransport, collector: MessageCollector| async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
        loop {
            publisher
                .publish_broadcast(&create_test_broadcast("ready"))
                .await
                .unwrap();
            if collector.wait_for_broadcast(100).await.is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Sentinel listener did not reconnect"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        collector.clear().await;
    };
    wait_ready(receiver.clone(), collector.clone()).await;
    for serial in 0..128 {
        let mut message = create_test_broadcast("before");
        message.message = serial.to_string();
        receiver.publish_broadcast(&message).await.unwrap();
    }
    assert!(
        wait_for_condition(
            || async { collector.get_broadcasts().await.len() == 128 },
            5000
        )
        .await
    );
    assert_eq!(
        collector
            .get_broadcasts()
            .await
            .iter()
            .map(|item| item.message.parse::<usize>().unwrap())
            .collect::<Vec<_>>(),
        (0..128).collect::<Vec<_>>()
    );
    redis::cmd("SENTINEL")
        .arg("FAILOVER")
        .arg("fanout")
        .query_async::<()>(&mut sentinel)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    loop {
        let primary: Vec<String> = redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg("fanout")
            .query_async(&mut sentinel)
            .await
            .unwrap();
        if primary[1].parse::<u16>().unwrap() != old_port {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Sentinel did not promote replica"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let old_container = format!("sockudo-perf-fanout-sentinel-{old_port}");
    struct Restart(String);
    impl Drop for Restart {
        fn drop(&mut self) {
            let _ = std::process::Command::new("podman")
                .args(["start", &self.0])
                .stdout(std::process::Stdio::null())
                .status();
        }
    }
    let restart = Restart(old_container.clone());
    assert!(
        std::process::Command::new("podman")
            .args(["stop", "--time", "1", &old_container])
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(wait_for_condition(|| async { gaps.load(Ordering::SeqCst) > 0 }, 10000).await);
    collector.clear().await;
    let publisher = RedisTransport::new(config).await.unwrap();
    wait_ready(publisher.clone(), collector.clone()).await;
    for serial in 128..256 {
        let mut message = create_test_broadcast("after");
        message.message = serial.to_string();
        publisher.publish_broadcast(&message).await.unwrap();
    }
    assert!(
        wait_for_condition(
            || async { collector.get_broadcasts().await.len() == 128 },
            5000
        )
        .await
    );
    assert_eq!(
        collector
            .get_broadcasts()
            .await
            .iter()
            .map(|item| item.message.parse::<usize>().unwrap())
            .collect::<Vec<_>>(),
        (128..256).collect::<Vec<_>>()
    );
    publisher
        .publish_request(&create_test_request())
        .await
        .unwrap();
    assert!(collector.wait_for_response(5000).await.is_some());
    receiver.check_health().await.unwrap();
    drop(restart);
}

#[cfg(feature = "redis-cluster")]
#[tokio::test]
#[ignore = "requires SOCKUDO_REDIS_CLUSTER_TEST_NODES"]
async fn redis_cluster_overload_keeps_control_live_and_drains_accepted_prefix() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::Semaphore;
    for sharded in [false, true] {
        let transport =
            sockudo_adapter::transports::RedisClusterTransport::new(cluster_config(sharded))
                .await
                .unwrap();
        let collector = MessageCollector::new();
        let mut handlers = create_test_handlers(collector.clone());
        let gap = Arc::new(AtomicUsize::new(0));
        let seen_gap = gap.clone();
        handlers.on_ingress_gap = Arc::new(move |app, channel| {
            assert!(app.is_none_or(|app| app == "test-app"));
            assert!(channel.is_none_or(|channel| channel == "test-channel"));
            seen_gap.fetch_add(1, Ordering::SeqCst);
        });
        let permits = Arc::new(Semaphore::new(0));
        let handler_permits = permits.clone();
        let collected = collector.clone();
        handlers.on_broadcast = Arc::new(move |message| {
            let permits = handler_permits.clone();
            let collected = collected.clone();
            Box::pin(async move {
                let _permit = permits.acquire().await.unwrap();
                collected.collect_broadcast(message).await;
            })
        });
        transport.start_listeners(handlers).await.unwrap();
        // start_listeners awaits per-shard subscription acknowledgements.
        for serial in 0..1024 {
            let mut message = create_test_broadcast("bounded");
            message.message = serial.to_string();
            transport.publish_broadcast(&message).await.unwrap();
        }
        assert!(wait_for_condition(|| async { gap.load(Ordering::SeqCst) > 0 }, 10000).await);
        transport
            .publish_request(&create_test_request())
            .await
            .unwrap();
        assert!(collector.wait_for_response(5000).await.is_some());
        tokio::time::sleep(Duration::from_millis(300)).await;
        permits.add_permits(1);
        assert!(
            wait_for_condition(
                || async { collector.get_broadcasts().await.len() >= 65 },
                10000
            )
            .await
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        let received = collector.get_broadcasts().await;
        assert_eq!(
            received.len(),
            65,
            "one active handler plus 64 admitted records"
        );
        assert_eq!(
            received
                .iter()
                .map(|message| message.message.parse::<usize>().unwrap())
                .collect::<Vec<_>>(),
            (0..65).collect::<Vec<_>>()
        );
    }
}

#[cfg(feature = "redis-cluster")]
#[tokio::test]
#[ignore = "restarts only explicit audit Redis Cluster fixture containers"]
async fn redis_cluster_outage_invalidates_continuity_and_reconnects() {
    use sockudo_adapter::transports::RedisClusterTransport;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let containers = std::env::var("SOCKUDO_REDIS_CLUSTER_OUTAGE_CONTAINERS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(containers.len(), 3);
    assert!(
        containers
            .iter()
            .all(|name| name.starts_with("sockudo-perf-"))
    );
    struct Restart(Vec<String>);
    impl Drop for Restart {
        fn drop(&mut self) {
            let _ = std::process::Command::new("podman")
                .arg("start")
                .args(&self.0)
                .stdout(std::process::Stdio::null())
                .status();
        }
    }
    for sharded in [false, true] {
        let receiver = RedisClusterTransport::new(cluster_config(sharded))
            .await
            .unwrap();
        let collector = MessageCollector::new();
        let gaps = Arc::new(AtomicUsize::new(0));
        let seen_gaps = gaps.clone();
        let mut handlers = create_test_handlers(collector.clone());
        handlers.on_ingress_gap = Arc::new(move |_, _| {
            seen_gaps.fetch_add(1, Ordering::SeqCst);
        });
        receiver.start_listeners(handlers).await.unwrap();
        receiver
            .publish_broadcast(&create_test_broadcast("before"))
            .await
            .unwrap();
        assert!(collector.wait_for_broadcast(5000).await.is_some());
        let restart = Restart(containers.clone());
        assert!(
            std::process::Command::new("podman")
                .args(["stop", "--time", "1"])
                .args(&containers)
                .stdout(std::process::Stdio::null())
                .status()
                .unwrap()
                .success()
        );
        assert!(wait_for_condition(|| async { gaps.load(Ordering::SeqCst) > 0 }, 10000).await);
        drop(restart);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
        loop {
            collector.clear().await;
            if receiver
                .publish_broadcast(&create_test_broadcast("ready"))
                .await
                .is_ok()
                && collector.wait_for_broadcast(100).await.is_some()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "existing cluster listener must reconnect"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        collector.clear().await;
        for serial in 0..128 {
            let mut message = create_test_broadcast("after");
            message.message = serial.to_string();
            receiver.publish_broadcast(&message).await.unwrap();
        }
        assert!(
            wait_for_condition(
                || async { collector.get_broadcasts().await.len() == 128 },
                10000
            )
            .await
        );
        assert_eq!(
            collector
                .get_broadcasts()
                .await
                .iter()
                .map(|message| message.message.parse::<usize>().unwrap())
                .collect::<Vec<_>>(),
            (0..128).collect::<Vec<_>>()
        );
        receiver
            .publish_request(&create_test_request())
            .await
            .unwrap();
        assert!(collector.wait_for_response(5000).await.is_some());
        receiver.check_health().await.unwrap();
    }
}
